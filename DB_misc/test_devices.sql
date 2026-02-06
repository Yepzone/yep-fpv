-- 检查所有设备
SELECT device_id, is_active, skip_scan, created_at
FROM fpv.devices
ORDER BY created_at;

-- 专门检查设备13fa
SELECT * FROM fpv.devices WHERE device_id = '13fa';

-- 检查设备13fa的会话
SELECT COUNT(*) FROM fpv.sessions WHERE device_id = '13fa';