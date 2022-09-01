//
// Created by jinghuan on 5/24/21.
//

#include "rocksdb/utilities/report_agent.h"

#include "rocksdb/utilities/DOTA_tuner.h"

namespace ROCKSDB_NAMESPACE {
ReporterAgent::~ReporterAgent() {
  {
    std::unique_lock<std::mutex> lk(mutex_);
    stop_ = true;
    stop_cv_.notify_all();
  }
  reporting_thread_.join();
}
void ReporterAgent::InsertNewTuningPoints(ChangePoint point) {
  std::cout << "can't use change point @ " << point.change_timing
            << " Due to using default reporter" << std::endl;
};
void ReporterAgent::DetectAndTuning(int /*secs_elapsed*/) {}
Status ReporterAgent::ReportLine(int secs_elapsed,
                                 int total_ops_done_snapshot) {
  std::string report = std::to_string(secs_elapsed) + "," +
                       std::to_string(total_ops_done_snapshot - last_report_);
  auto s = report_file_->Append(report);
  return s;
}

void ReporterAgentWithTuning::DetectChangesPoints(int sec_elapsed) {
  std::vector<ChangePoint> change_points;
  if (applying_changes) {
    return;
  }
  tuner->DetectTuningOperations(sec_elapsed, &change_points);
  ApplyChangePointsInstantly(&change_points);
}

void ReporterAgentWithTuning::DetectAndTuning(int secs_elapsed) {
  if (secs_elapsed % tuning_gap_secs_ == 0) {
    DetectChangesPoints(secs_elapsed);
    //    this->running_db_->immutable_db_options().job_stats->clear();
    last_metrics_collect_secs = secs_elapsed;
  }
  if (tuning_points.empty() ||
      tuning_points.front().change_timing < secs_elapsed) {
    return;
  } else {
    PopChangePoints(secs_elapsed);
  }
}

Status ReporterAgentWithTuning::ReportLine(int secs_elapsed,
                                           int total_ops_done_snapshot) {
  auto opt = this->running_db_->GetOptions();

  std::string report = std::to_string(secs_elapsed) + "," +
                       std::to_string(total_ops_done_snapshot - last_report_) + "," +
                       std::to_string(opt.write_buffer_size >> 20) + "," +
                       std::to_string(opt.max_background_jobs);
  auto s = report_file_->Append(report);
  return s;
}
void ReporterAgentWithTuning::UseFEATTuner(bool TEA_enable, bool FEA_enable) {
  tuner.release();
  tuner.reset(new FEAT_Tuner(options_when_boost, running_db_, &last_report_,
                             &total_ops_done_, env_, tuning_gap_secs_,
                             TEA_enable, FEA_enable));
};

Status update_db_options(
    DBImpl* running_db_,
    std::unordered_map<std::string, std::string>* new_db_options,
    bool* applying_changes, Env* /*env*/) {
  *applying_changes = true;
  Status s = running_db_->SetDBOptions(*new_db_options);
  free(new_db_options);
  *applying_changes = false;
  return s;
}

Status update_cf_options(
    DBImpl* running_db_,
    std::unordered_map<std::string, std::string>* new_cf_options,
    bool* applying_changes, Env* /*env*/) {
  *applying_changes = true;
  Status s = running_db_->SetOptions(*new_cf_options);
  free(new_cf_options);
  *applying_changes = false;
  return s;
}

Status SILK_pause_compaction(DBImpl* running_db_, bool* stopped) {
  Status s = running_db_->PauseBackgroundWork();
  *stopped = true;
  return s;
}

Status SILK_resume_compaction(DBImpl* running_db_, bool* stopped) {
  Status s = running_db_->ContinueBackgroundWork();
  *stopped = false;
  return s;
}

void ReporterAgentWithTuning::ApplyChangePointsInstantly(
    std::vector<ChangePoint>* points) {
  std::unordered_map<std::string, std::string>* new_cf_options;
  std::unordered_map<std::string, std::string>* new_db_options;

  new_cf_options = new std::unordered_map<std::string, std::string>();
  new_db_options = new std::unordered_map<std::string, std::string>();
  if (points->empty()) {
    return;
  }

  for (auto point : *points) {
    if (point.db_width) {
      new_db_options->emplace(point.opt, point.value);
    } else {
      new_cf_options->emplace(point.opt, point.value);
    }
  }
  points->clear();
  Status s;
  if (!new_db_options->empty()) {
    //    std::thread t();
    std::thread t(update_db_options, running_db_, new_db_options,
                  &applying_changes, env_);
    t.detach();
  }
  if (!new_cf_options->empty()) {
    std::thread t(update_cf_options, running_db_, new_cf_options,
                  &applying_changes, env_);
    t.detach();
  }
}

ReporterAgentWithTuning::ReporterAgentWithTuning(DBImpl* running_db, Env* env,
                                                 const std::string& fname,
                                                 uint64_t report_interval_secs,
                                                 uint64_t dota_tuning_gap_secs)
    : ReporterAgent(env, fname, report_interval_secs, DOTAHeader()),
      options_when_boost(running_db->GetOptions()) {
  tuning_points = std::vector<ChangePoint>();
  tuning_points.clear();
  std::cout << "using reporter agent with change points." << std::endl;
  if (running_db == nullptr) {
    std::cout << "Missing parameter db_ to apply changes" << std::endl;
    abort();
  } else {
    running_db_ = running_db;
  }
  this->tuning_gap_secs_ = std::max(dota_tuning_gap_secs, report_interval_secs);
  this->last_metrics_collect_secs = 0;
  this->last_compaction_thread_len = 0;
  this->last_flush_thread_len = 0;
  tuner.reset(new DOTA_Tuner(options_when_boost, running_db_, &last_report_,
                             &total_ops_done_, env_, tuning_gap_secs_));
  tuner->ResetTuner();
  this->applying_changes = false;
}

inline double average(std::vector<double>& v) {
  assert(!v.empty());
  return accumulate(v.begin(), v.end(), 0.0) / v.size();
}

void ReporterAgentWithTuning::PopChangePoints(int secs_elapsed) {
  std::vector<ChangePoint> valid_point;
  for (auto it = tuning_points.begin(); it != tuning_points.end(); it++) {
    if (it->change_timing <= secs_elapsed) {
      if (running_db_ != nullptr) {
        valid_point.push_back(*it);
      }
      tuning_points.erase(it--);
    }
  }
  ApplyChangePointsInstantly(&valid_point);
}

void ReporterWithMoreDetails::DetectAndTuning(int secs_elapsed) {
  //  report_file_->Append(",");
  //  ReportLine(secs_elapsed, total_ops_done_);
  secs_elapsed++;
}

Status ReporterAgentWithSILK::ReportLine(int secs_elapsed,
                                         int total_ops_done_snapshot) {
  // copy from SILK https://github.com/theoanab/SILK-USENIXATC2019
  // //check the current bandwidth for user operations
  long cur_throughput = (total_ops_done_snapshot - last_report_);
  long cur_bandwidth_user_ops_MBPS =
      cur_throughput * FLAGS_value_size / 1000000;

  // SILK TESTING the Pause compaction work functionality
  if (!pausedcompaction &&
      cur_bandwidth_user_ops_MBPS > FLAGS_SILK_bandwidth_limitation * 0.75) {
    // SILK Consider this a load peak
    //    running_db_->PauseCompactionWork();
    //    pausedcompaction = true;
    pausedcompaction = true;
    std::thread t(SILK_pause_compaction, running_db_, &pausedcompaction);
    t.detach();

  } else if (pausedcompaction && cur_bandwidth_user_ops_MBPS <=
                                     FLAGS_SILK_bandwidth_limitation * 0.75) {
    std::thread t(SILK_resume_compaction, running_db_, &pausedcompaction);
    t.detach();
  }

  long cur_bandiwdth_compaction_MBPS =
      FLAGS_SILK_bandwidth_limitation -
      cur_bandwidth_user_ops_MBPS;  // measured 200MB/s SSD bandwidth on XEON.
  if (cur_bandiwdth_compaction_MBPS < 10) {
    cur_bandiwdth_compaction_MBPS = 10;
  }
  if (abs(prev_bandwidth_compaction_MBPS - cur_bandiwdth_compaction_MBPS) >=
      10) {
    auto opt = running_db_->GetOptions();
    opt.rate_limiter.get()->SetBytesPerSecond(cur_bandiwdth_compaction_MBPS *
                                              1000 * 1000);
    prev_bandwidth_compaction_MBPS = cur_bandiwdth_compaction_MBPS;
  }
  // Adjust the tuner from SILK before reporting
  std::string report = std::to_string(secs_elapsed) + "," +
                       std::to_string(total_ops_done_snapshot - last_report_) + "," +
                       std::to_string(cur_bandwidth_user_ops_MBPS);
  auto s = report_file_->Append(report);
  return s;
}

ReporterAgentWithSILK::ReporterAgentWithSILK(DBImpl* running_db, Env* env,
                                             const std::string& fname,
                                             uint64_t report_interval_secs,
                                             int32_t value_size,
                                             int32_t bandwidth_limitation)
    : ReporterAgent(env, fname, report_interval_secs) {
  std::cout << "SILK enabled, Disk bandwidth has been set to: "
            << bandwidth_limitation << std::endl;
  running_db_ = running_db;
  this->FLAGS_value_size = value_size;
  this->FLAGS_SILK_bandwidth_limitation = bandwidth_limitation;
}

};  // namespace ROCKSDB_NAMESPACE
