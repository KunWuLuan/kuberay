package utils

import "time"

// Ray session directory related constants.
const (
	RaySessionDirLogDirName  = "logs"
	RaySessionDirMetaDirName = "meta"
)

// Local Ray runtime paths.
const (
	RaySessionLatestPath = "/tmp/ray/session_latest"
	RayNodeIDPath        = "/tmp/ray/raylet_node_id"
)

// OSS meta file keys used by history server.
const (
	OssMetaFileBasicInfo = "ack__basicinfo"

	OssMetaFileNodeSummaryKey                   = "restful__nodes_view_summary"
	OssMetaFileNodePrefix                       = "restful__nodes_"
	OssMetaFileJobTaskDetailPrefix              = "restful__api__v0__tasks_detail_job_id_"
	OssMetaFileJobTaskSummarizeByFuncNamePrefix = "restful__api__v0__tasks_summarize_by_func_name_job_id_"
	OssMetaFileJobTaskSummarizeByLineagePrefix  = "restful__api__v0__tasks_summarize_by_lineage_job_id_"
	OssMetaFileJobDatasetsPrefix                = "restful__api__data__datasets_job_id_"
	OssMetaFileNodeLogsPrefix                   = "restful__api__v0__logs_node_id_"
	OssMetaFileClusterStatus                    = "restful__api__cluster_status"
	OssMetaFileLogicalActors                    = "restful__logical__actors"
	OssMetaFileAllTasksDetail                   = "restful__api__v0__tasks_detail"
	OssMetaFileEvents                           = "restful__events"
	OssMetaFilePlacementGroups                  = "restful__api__v0__placement_groups_detail"
	OssMetaFileClusterSessionName               = "static__api__cluster_session_name"
	OssMetaFileJobs                             = "restful__api__jobs"
	OssMetaFileApplications                     = "restful__api__serve__applications"
)

// Ray history server log file name.
const RayHistoryServerLogName = "historyserver-ray.log"

const (
	// DefaultMaxRetryAttempts controls how many times we retry reading
	// local Ray metadata files (e.g. session dir, node id) before failing.
	DefaultMaxRetryAttempts = 3
	// DefaultInitialRetryDelay is the base delay before the first retry.
	// Subsequent retries use an exponential backoff based on this value.
	DefaultInitialRetryDelay = 5 * time.Second
)
