-- ============================================================================
-- FPV 数据库架构 - 视频采集数据管理系统
-- Schema: fpv
-- ============================================================================
-- 创建时间: 2025/12/23 CST
-- 说明: 管理OSS上的视频采集会话和视频段数据
-- ============================================================================

-- 1. 创建 Schema
CREATE SCHEMA IF NOT EXISTS fpv;

-- ============================================================================
-- 表 1: fpv.devices (设备配置表)
-- ============================================================================
-- 说明: 存储设备配置信息，包括视频压缩率、是否跳过扫描等
-- ============================================================================

CREATE TABLE IF NOT EXISTS fpv.devices
(
    -- 主键
    device_id
    VARCHAR
(
    32
) PRIMARY KEY,

    -- 设备信息
    device_model VARCHAR
(
    128
),

    -- 视频压缩配置 (用于估算时长)
    mb_per_10min DECIMAL
(
    10,
    2
) DEFAULT 600.0, -- 每10分钟的视频大小(MB)

-- 状态标志
    is_active BOOLEAN DEFAULT TRUE, -- 设备是否启用
    skip_scan BOOLEAN DEFAULT FALSE, -- 是否跳过扫描 (替代硬编码列表)

-- 时间戳 (UTC)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    updated_at TIMESTAMP
                         WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
    );

-- 索引
CREATE INDEX IF NOT EXISTS idx_devices_active ON fpv.devices (is_active);
CREATE INDEX IF NOT EXISTS idx_devices_skip_scan ON fpv.devices (skip_scan);

-- 注释
COMMENT
ON TABLE fpv.devices IS '设备配置表，存储设备元数据和扫描配置';
COMMENT
ON COLUMN fpv.devices.device_id IS '设备唯一标识符';
COMMENT
ON COLUMN fpv.devices.mb_per_10min IS '每10分钟视频的平均大小(MB)，用于估算视频时长';
COMMENT
ON COLUMN fpv.devices.skip_scan IS '是否在扫描时跳过此设备';

-- ============================================================================
-- 表 2: fpv.sessions (会话元数据表)
-- ============================================================================
-- 说明: 存储从OSS metadata.json提取的会话级别元数据
-- ============================================================================

CREATE TABLE IF NOT EXISTS fpv.sessions
(
    -- 主键
    id
    UUID
    PRIMARY
    KEY
    DEFAULT
    gen_random_uuid
(
),

    -- 业务唯一标识符
    session_id VARCHAR
(
    64
) NOT NULL UNIQUE,

    -- 外键: 关联设备
    device_id VARCHAR
(
    32
) NOT NULL REFERENCES fpv.devices
(
    device_id
),

    -- 时间信息 (从session_id解析, UTC时间)
    collect_date DATE NOT NULL, -- 采集日期
    collect_time TIME NOT NULL, -- 采集时间
    start_time_utc TIMESTAMP WITH TIME ZONE NOT NULL, -- 会话开始时间(UTC)
    end_time_utc TIMESTAMP WITH TIME ZONE NOT NULL, -- 会话结束时间(UTC)

                               -- 任务/地点信息 (来自metadata.json)
                               task_description VARCHAR (255), -- 任务描述
    scene VARCHAR
(
    64
), -- 场景
    collect_site VARCHAR
(
    128
), -- 采集地点
    operator_info JSONB, -- 操作员信息 (JSON格式)

-- 设备配置信息 (来自metadata.json)
    device_model VARCHAR
(
    128
), -- 设备型号
    platform VARCHAR
(
    64
), -- 平台
    resolution VARCHAR
(
    32
), -- 视频分辨率
    fps INTEGER, -- 帧率
    num_cameras INTEGER, -- 摄像头数量

-- 原始JSON数据
    raw_metadata_json JSONB NOT NULL, -- metadata.json的完整内容

-- 时间戳 (UTC)
    created_at TIMESTAMP
                           WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    updated_at TIMESTAMP
                           WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
    );

-- 索引
CREATE INDEX IF NOT EXISTS idx_sessions_device_id ON fpv.sessions (device_id);
CREATE INDEX IF NOT EXISTS idx_sessions_collect_date ON fpv.sessions (collect_date);
CREATE INDEX IF NOT EXISTS idx_sessions_updated_at ON fpv.sessions (updated_at);

-- 注释
COMMENT
ON TABLE fpv.sessions IS '会话元数据表，存储从metadata.json提取的会话级别信息';
COMMENT
ON COLUMN fpv.sessions.session_id IS '会话唯一标识符，格式: session_YYYYMMDD_HHMMSS_微秒';
COMMENT
ON COLUMN fpv.sessions.collect_date IS '采集日期(从session_id解析)';
COMMENT
ON COLUMN fpv.sessions.collect_time IS '采集时间(从session_id解析)';
COMMENT
ON COLUMN fpv.sessions.operator_info IS '操作员信息(JSON), 如: {"operator_height": 175}';
COMMENT
ON COLUMN fpv.sessions.raw_metadata_json IS '完整的metadata.json原始内容';

-- ============================================================================
-- 表 3: fpv.segments (视频段表)
-- ============================================================================
-- 说明: 存储视频段信息，每行表示一对视频(down + front摄像头)
-- ============================================================================

CREATE TABLE IF NOT EXISTS fpv.segments
(
    -- 主键
    id
    UUID
    PRIMARY
    KEY
    DEFAULT
    gen_random_uuid
(
),

    -- 外键: 关联会话
    session_id VARCHAR
(
    64
) NOT NULL REFERENCES fpv.sessions
(
    session_id
),

    -- 视频段编号
    segment_number VARCHAR
(
    4
) NOT NULL, -- 格式: "0001", "0002", ...

-- Down 摄像头视频信息
    down_file_name VARCHAR
(
    255
),
    down_oss_path TEXT NOT NULL,
    down_file_size_bytes BIGINT,

    -- Front 摄像头视频信息
    front_file_name VARCHAR
(
    255
),
    front_oss_path TEXT NOT NULL,
    front_file_size_bytes BIGINT,

    -- 时间戳 (UTC)
    created_at TIMESTAMP WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
    updated_at TIMESTAMP
                         WITH TIME ZONE DEFAULT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),

    -- 唯一约束: 同一会话的同一段号只能有一条记录
    CONSTRAINT uq_session_segment UNIQUE
(
    session_id,
    segment_number
)
    );

-- 索引
CREATE INDEX IF NOT EXISTS idx_segments_session_id ON fpv.segments (session_id);
CREATE INDEX IF NOT EXISTS idx_segments_updated_at ON fpv.segments (updated_at);

-- 注释
COMMENT
ON TABLE fpv.segments IS '视频段表，存储配对的视频文件信息(down + front)';
COMMENT
ON COLUMN fpv.segments.segment_number IS '视频段编号，格式为4位数字字符串';
COMMENT
ON COLUMN fpv.segments.down_oss_path IS 'Down摄像头视频的OSS路径';
COMMENT
ON COLUMN fpv.segments.front_oss_path IS 'Front摄像头视频的OSS路径';
COMMENT
ON CONSTRAINT uq_session_segment ON fpv.segments IS '确保同一会话的同一段号唯一';

-- ============================================================================
-- 视图 1: fpv.segments_view (带计算字段的视图)
-- ============================================================================
-- 说明: 在segments基础上计算total_size_mb和estimated_duration_min
-- ============================================================================

CREATE
OR REPLACE VIEW fpv.segments_view AS
SELECT seg.id,
       seg.session_id,
       seg.segment_number,

       -- 文件信息
       seg.down_file_name,
       seg.down_oss_path,
       seg.down_file_size_bytes,
       seg.front_file_name,
       seg.front_oss_path,
       seg.front_file_size_bytes,

       -- 计算字段: total_size_mb (总文件大小，单位MB)
       ROUND(
               (COALESCE(seg.down_file_size_bytes, 0) +
                COALESCE(seg.front_file_size_bytes, 0)):: NUMERIC / (1024.0 * 1024.0),
               2
       ) AS total_size_mb,

       -- 计算字段: estimated_duration_min (估算时长，单位分钟，最大10.0)
       -- 公式: (total_size_mb / mb_per_10min) * 10.0
       LEAST(
               ROUND(
                       (
                           (COALESCE(seg.down_file_size_bytes, 0) +
                            COALESCE(seg.front_file_size_bytes, 0)):: NUMERIC / (1024.0 * 1024.0)
                           / COALESCE (dev.mb_per_10min, 600.0) ) * 10.0,
            1
        ),
               10.0
       ) AS estimated_duration_min,

       -- 关联的session和device信息
       sess.device_id,
       sess.collect_date,
       sess.collect_time,

       -- 时间戳
       seg.created_at,
       seg.updated_at

FROM fpv.segments seg
         JOIN fpv.sessions sess ON seg.session_id = sess.session_id
         JOIN fpv.devices dev ON sess.device_id = dev.device_id;

COMMENT
ON VIEW fpv.segments_view IS '视频段视图，包含计算字段total_size_mb和estimated_duration_min';

-- ============================================================================
-- 视图 2: fpv.segments_csv_export (CSV导出专用视图)
-- ============================================================================
-- 说明: 格式化输出，与现有CSV格式兼容
-- ============================================================================

CREATE
OR REPLACE VIEW fpv.segments_csv_export AS
SELECT seg.updated_at AS updated_at,
       sess.collect_date AS date,
    sess.collect_time AS time,
    sess.device_id,
    seg.segment_number,
    '待审批' AS approval_status,  -- preset值，用于QA工作流
    seg.down_oss_path,
    seg.front_oss_path,
    seg.session_id,


    -- 格式化文件大小 (格式: "1234.56 MB")
    CONCAT(
        ROUND(
            (COALESCE(seg.down_file_size_bytes, 0) + COALESCE(seg.front_file_size_bytes, 0))::NUMERIC / (1024.0 * 1024.0),
            2
        ),
        ' MB'
    ) AS filesize,

    -- 估算时长 (单位: 分钟, 最大10.0)
    LEAST(
        ROUND(
            (
                (COALESCE(seg.down_file_size_bytes, 0) + COALESCE(seg.front_file_size_bytes, 0))::NUMERIC / (1024.0 * 1024.0)
                / COALESCE(dev.mb_per_10min, 600.0)
            ) * 10.0,
            1
        ),
        10.0
    ) AS estimated_duration

FROM fpv.segments seg
    JOIN fpv.sessions sess
ON seg.session_id = sess.session_id
    JOIN fpv.devices dev ON sess.device_id = dev.device_id
ORDER BY sess.collect_date DESC, sess.collect_time DESC, seg.segment_number;

COMMENT
ON VIEW fpv.segments_csv_export IS 'CSV导出专用视图，格式与现有CSV兼容，包含preset的approval_status字段';

-- ============================================================================
-- 视图 3: fpv.sessions_summary (会话摘要视图)
-- ============================================================================
-- 说明: 提供会话级别的汇总信息，包括视频段统计
-- ============================================================================

CREATE
OR REPLACE VIEW fpv.sessions_summary AS
SELECT sess.id,
       sess.session_id,
       sess.device_id,
       -- 转换为北京时间（UTC+8）
       (sess.collect_date + sess.collect_time + INTERVAL '8 hours')::date AS collect_date, (sess.collect_date + sess.collect_time + INTERVAL '8 hours')::time AS collect_time,

    -- 会话时长(小时) 
    ROUND(
        EXTRACT(EPOCH FROM (sess.end_time_utc - sess.start_time_utc)) / 3600.0,
        2
                      ) AS duration_hours,

       -- 任务信息
       sess.task_description,
       sess.scene,
       sess.collect_site,

       -- 视频段统计
       COUNT(seg.id)                                                                                           AS segment_count,
       SUM(COALESCE(seg.down_file_size_bytes, 0) + COALESCE(seg.front_file_size_bytes, 0)) /
       (1024.0 * 1024.0)                                                                                       AS total_size_mb,

       -- 时间戳
       sess.created_at,
       sess.updated_at

FROM fpv.sessions sess
         LEFT JOIN fpv.segments seg ON sess.session_id = seg.session_id
GROUP BY sess.id,
         sess.session_id,
         sess.device_id,
         sess.collect_date,
         sess.collect_time,
         sess.start_time_utc,
         sess.end_time_utc,
         sess.task_description,
         sess.scene,
         sess.collect_site,
         sess.created_at,
         sess.updated_at;

COMMENT
ON VIEW fpv.sessions_summary IS '会话摘要视图，包含会话时长和视频段统计信息';

-- ============================================================================
-- 初始化数据
-- ============================================================================

-- 插入默认设备配置
INSERT INTO fpv.devices (device_id, mb_per_10min, skip_scan)
VALUES ('default', 600.0, FALSE) ON CONFLICT (device_id) DO NOTHING;

-- 插入需要跳过的设备
INSERT INTO fpv.devices (device_id, mb_per_10min, skip_scan)
VALUES ('stereo_cam0', 600.0, TRUE),
       ('test', 600.0, TRUE),
       ('a027', 600.0, TRUE) ON CONFLICT (device_id) DO NOTHING;


