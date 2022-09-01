//
// Created by jinghuan on 8/24/21.
//

#ifndef ROCKSDB_DOTA_TUNER_H
#define ROCKSDB_DOTA_TUNER_H
#pragma once
#include <iostream>

namespace ROCKSDB_NAMESPACE {

enum ThreadStallLevels : int {
  //  kLowFlush,
  kL0Stall,
  kPendingBytes,
  kGoodArea,
  kIdle,
  kBandwidthCongestion
};
enum BatchSizeStallLevels : int {
  kTinyMemtable,  // tiny memtable
  kStallFree,
  kOverFrequent
};

struct SystemScores {
  // Memory Component
  uint64_t memtable_speed;   // MB per sec
  double active_size_ratio;  // active size / total memtable size
  int immutable_number;      // NonFlush number
  // Flushing
  double flush_speed_avg;
  double flush_min;
  double flush_speed_var;
  // Compaction speed
  double l0_num;
  // LSM  size
  double l0_drop_ratio;
  double estimate_compaction_bytes;  // given by the system, divided by the soft
                                     // limit
  // System metrics
  double disk_bandwidth;  // avg
  double flush_idle_time;
  double flush_gap_time;
  double compaction_idle_time;  // calculate by idle calculating,flush and
                                // compaction stats separately
  int flush_numbers;

  SystemScores() {
    memtable_speed = 0.0;
    active_size_ratio = 0.0;
    immutable_number = 0;
    flush_speed_avg = 0.0;
    flush_min = 9999999;
    flush_speed_var = 0.0;
    l0_num = 0.0;
    l0_drop_ratio = 0.0;
    estimate_compaction_bytes = 0.0;
    disk_bandwidth = 0.0;
    compaction_idle_time = 0.0;
    flush_numbers = 0;
    flush_gap_time = 0;
  }
  void Reset() {
    memtable_speed = 0.0;
    active_size_ratio = 0.0;
    immutable_number = 0;
    flush_speed_avg = 0.0;
    flush_speed_var = 0.0;
    l0_num = 0.0;
    l0_drop_ratio = 0.0;
    estimate_compaction_bytes = 0.0;
    disk_bandwidth = 0.0;
    compaction_idle_time = 0.0;
    flush_numbers = 0;
    flush_gap_time = 0;
  }
  SystemScores operator-(const SystemScores& a);
  SystemScores operator+(const SystemScores& a);
  SystemScores operator/(const int& a);
};

typedef SystemScores ScoreGradient;

struct ChangePoint {
  std::string opt;
  std::string value;
  int change_timing;
  bool db_width;
};
enum OpType : int { kLinearIncrease, kHalf, kKeep };
struct TuningOP {
  OpType BatchOp;
  OpType ThreadOp;
};
class DOTA_Tuner {
 protected:
  const Options default_opts;
  uint64_t tuning_rounds;
  Options current_opt;
  Version* version;
  ColumnFamilyData* cfd;
  VersionStorageInfo* vfs;
  DBImpl* running_db_;
  int64_t* last_report_ptr;
  std::atomic<int64_t>* total_ops_done_ptr_;
  std::deque<SystemScores> scores;
  std::vector<ScoreGradient> gradients;
  int current_sec;
  uint64_t flush_list_accessed, compaction_list_accessed;
  ThreadStallLevels last_thread_states;
  BatchSizeStallLevels last_batch_stat;
  std::shared_ptr<std::vector<FlushMetrics>> flush_list_from_opt_ptr;
  std::shared_ptr<std::vector<QuicksandMetrics>> compaction_list_from_opt_ptr;
  SystemScores max_scores;
  SystemScores avg_scores;
  uint64_t last_flush_thread_len;
  uint64_t last_compaction_thread_len;
  Env* env_;
  double tuning_gap;
  int double_ratio = 2;
  uint64_t last_unflushed_bytes = 0;
  const int score_array_len = 600 / tuning_gap;
  double idle_threshold = 2.5;
  double FEA_gap_threshold = 1;
  double TEA_slow_flush = 0.5;
  uint64_t last_non_zero_flush = 0;
  void UpdateSystemStats() { UpdateSystemStats(running_db_); }

 public:
  DOTA_Tuner(const Options opt, DBImpl* running_db, int64_t* last_report_op_ptr,
             std::atomic<int64_t>* total_ops_done_ptr, Env* env,
             uint64_t gap_sec)
      : default_opts(opt),
        tuning_rounds(0),
        running_db_(running_db),
        scores(),
        gradients(0),
        current_sec(0),
        flush_list_accessed(0),
        compaction_list_accessed(0),
        last_thread_states(kL0Stall),
        last_batch_stat(kTinyMemtable),
        flush_list_from_opt_ptr(running_db->immutable_db_options().flush_stats),
        compaction_list_from_opt_ptr(
            running_db->immutable_db_options().job_stats),
        max_scores(),
        last_flush_thread_len(0),
        last_compaction_thread_len(0),
        env_(env),
        tuning_gap(gap_sec),
        core_num(running_db->immutable_db_options().core_number),
        max_memtable_size(
            running_db->immutable_db_options().max_memtable_size) {
    this->last_report_ptr = last_report_op_ptr;
    this->total_ops_done_ptr_ = total_ops_done_ptr;
  }
  void set_idle_ratio(double idle_ra) { idle_threshold = idle_ra; }
  void set_gap_threshold(double ng_threshold) {
    FEA_gap_threshold = ng_threshold;
  }
  void set_slow_flush_threshold(double sf_threshold) {
    this->TEA_slow_flush = sf_threshold;
  }
  virtual ~DOTA_Tuner();

  inline void UpdateMaxScore(SystemScores& current_score) {
    //    if (!scores.empty() &&
    //        current_score.memtable_speed > scores.front().memtable_speed * 2)
    //        {
    //      // this would be an error
    //      return;
    //    }

    if (current_score.memtable_speed > max_scores.memtable_speed) {
      max_scores.memtable_speed = current_score.memtable_speed;
    }
    if (current_score.active_size_ratio > max_scores.active_size_ratio) {
      max_scores.active_size_ratio = current_score.active_size_ratio;
    }
    if (current_score.immutable_number > max_scores.immutable_number) {
      max_scores.immutable_number = current_score.immutable_number;
    }

    if (current_score.flush_speed_avg > max_scores.flush_speed_avg) {
      max_scores.flush_speed_avg = current_score.flush_speed_avg;
    }
    if (current_score.flush_speed_var > max_scores.flush_speed_var) {
      max_scores.flush_speed_var = current_score.flush_speed_var;
    }
    if (current_score.l0_num > max_scores.l0_num) {
      max_scores.l0_num = current_score.l0_num;
    }
    if (current_score.l0_drop_ratio > max_scores.l0_drop_ratio) {
      max_scores.l0_drop_ratio = current_score.l0_drop_ratio;
    }
    if (current_score.estimate_compaction_bytes >
        max_scores.estimate_compaction_bytes) {
      max_scores.estimate_compaction_bytes =
          current_score.estimate_compaction_bytes;
    }
    if (current_score.disk_bandwidth > max_scores.disk_bandwidth) {
      max_scores.disk_bandwidth = current_score.disk_bandwidth;
    }
    if (current_score.flush_idle_time > max_scores.flush_idle_time) {
      max_scores.flush_idle_time = current_score.flush_idle_time;
    }
    if (current_score.compaction_idle_time > max_scores.compaction_idle_time) {
      max_scores.compaction_idle_time = current_score.compaction_idle_time;
    }
    if (current_score.flush_numbers > max_scores.flush_numbers) {
      max_scores.flush_numbers = current_score.flush_numbers;
    }
  }

  void ResetTuner() { tuning_rounds = 0; }
  void UpdateSystemStats(DBImpl* running_db) {
    current_opt = running_db->GetOptions();
    version = running_db->GetVersionSet()
                  ->GetColumnFamilySet()
                  ->GetDefault()
                  ->current();
    cfd = version->cfd();
    vfs = version->storage_info();
  }
  virtual void DetectTuningOperations(int secs_elapsed,
                                      std::vector<ChangePoint>* change_list);

  ScoreGradient CompareWithBefore() { return scores.back() - scores.front(); }
  ScoreGradient CompareWithBefore(SystemScores& past_score) {
    return scores.back() - past_score;
  }
  ScoreGradient CompareWithBefore(SystemScores& past_score,
                                  SystemScores& current_score) {
    return current_score - past_score;
  }
  ThreadStallLevels LocateThreadStates(SystemScores& score);
  BatchSizeStallLevels LocateBatchStates(SystemScores& score);

  const std::string memtable_size = "write_buffer_size";
  const std::string sst_size = "target_file_size_base";
  const std::string total_l1_size = "max_bytes_for_level_base";
  const std::string max_bg_jobs = "max_background_jobs";
  const std::string memtable_number = "max_write_buffer_number";

  const int core_num;
  int max_thread = core_num;
  const int min_thread = 2;
  uint64_t max_memtable_size;
  const uint64_t min_memtable_size = 64 << 20;

  SystemScores ScoreTheSystem();
  void AdjustmentTuning(std::vector<ChangePoint>* change_list,
                        SystemScores& score, ThreadStallLevels levels,
                        BatchSizeStallLevels stallLevels);
  TuningOP VoteForOP(SystemScores& current_score, ThreadStallLevels levels,
                     BatchSizeStallLevels stallLevels);
  void FillUpChangeList(std::vector<ChangePoint>* change_list, TuningOP op);
  void SetBatchSize(std::vector<ChangePoint>* change_list,
                    uint64_t target_value);
  void SetThreadNum(std::vector<ChangePoint>* change_list, int target_value);
};

enum Stage : int { kSlowStart, kStabilizing };
class FEAT_Tuner : public DOTA_Tuner {
 public:
  FEAT_Tuner(const Options opt, DBImpl* running_db, int64_t* last_report_op_ptr,
             std::atomic<int64_t>* total_ops_done_ptr, Env* env, int gap_sec,
             bool triggerTEA, bool triggerFEA)
      : DOTA_Tuner(opt, running_db, last_report_op_ptr, total_ops_done_ptr, env,
                   gap_sec),
        TEA_enable(triggerTEA),
        FEA_enable(triggerFEA),
        current_stage(kSlowStart) {
    flush_list_from_opt_ptr =
        this->running_db_->immutable_db_options().flush_stats;

    std::cout << "Using FEAT tuner.\n FEA is "
              << (FEA_enable ? "triggered" : "NOT triggered") << std::endl;
    std::cout << "TEA is " << (TEA_enable ? "triggered" : "NOT triggered")
              << std::endl;
  }
  void DetectTuningOperations(int secs_elapsed,
                              std::vector<ChangePoint>* change_list) override;
  ~FEAT_Tuner() override;

  TuningOP TuneByTEA();
  TuningOP TuneByFEA();

 private:
  bool TEA_enable;
  bool FEA_enable;
  SystemScores current_score_;
  SystemScores head_score_;
  std::deque<TuningOP> recent_ops;
  Stage current_stage;
  double bandwidth_congestion_threshold = 0.7;
  double slow_down_threshold = 0.75;
  double RO_threshold = 0.8;
  double LO_threshold = 0.7;
  double MO_threshold = 0.5;
  double batch_changing_frequency = 0.7;
  int congestion_threads = min_thread;
  //  int double_ratio = 4;
  SystemScores normalize(SystemScores& origin_score);

  inline const char* StageString(Stage v) {
    switch (v) {
      case kSlowStart:
        return "slow start";
        //      case kBoundaryDetection:
        //        return "Boundary Detection";
      case kStabilizing:
        return "Stabilizing";
    }
    return "unknown operation";
  }
  void CalculateAvgScore();
};
inline const char* OpString(OpType v) {
  switch (v) {
    case kLinearIncrease:
      return "Linear Increase";
    case kHalf:
      return "Half";
    case kKeep:
      return "Keep";
  }
  return "unknown operation";
}
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_DOTA_TUNER_H
