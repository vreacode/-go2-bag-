import os
import sys
import time
import threading
import logging
import shutil
from datetime import datetime, timedelta
import subprocess
import signal

DATA_ROOT = '/home/unitree/data'
LOG_ROOT = '/home/unitree/data_log'
RETENTION_DAYS = 7
INTERFACE_NAME = 'eth0'
MULTICAST_ADDR = '230.1.1.1'
MULTICAST_PORT = 1720
VIDEO_PREFIX = 'video'
VIDEO_EXT = '.mkv'
BAG_PREFIX = 'lidar'

# 全局变量用于线程间通信
video_gst_proc = None
lidar_proc = None
exit_event = threading.Event()

# 全局变量存储当前会话文件夹
current_session_folder = None
# 全局变量存储当前视频文件编号
current_video_number = 1
# 全局变量存储当前雷达bag文件编号
current_lidar_number = 1

# 获取开机时间戳（避免系统时间不准确）
BOOT_TIME = int(time.time())
BOOT_TIME_STR = datetime.fromtimestamp(BOOT_TIME).strftime('%Y%m%d_%H%M%S')

# 日志初始化
def setup_logger():
    # 使用全局会话文件夹名称，确保日志文件夹与数据文件夹对应
    global current_session_folder
    if current_session_folder is None:
        # 如果还没创建会话文件夹，先创建一个临时名称
        session_name = "temp_session"
    else:
        # 从会话文件夹路径中提取文件夹名
        session_name = os.path.basename(current_session_folder)
    
    log_dir = os.path.join(LOG_ROOT, session_name)
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, 'log.txt')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s',
        handlers=[logging.FileHandler(log_path), logging.StreamHandler(sys.stdout)]
    )

# 创建会话文件夹（只在启动时调用一次）
def create_session_folder():
    global current_session_folder
    # 检测已有文件夹，使用递增编号命名
    existing_folders = []
    if os.path.exists(DATA_ROOT):
        for item in os.listdir(DATA_ROOT):
            item_path = os.path.join(DATA_ROOT, item)
            # 只检测以session-开头的文件夹
            if os.path.isdir(item_path) and item.startswith('session-'):
                try:
                    # 提取编号部分，格式为 "session-001", "session-002" 等
                    num = int(item.split('-')[1])
                    existing_folders.append(num)
                except (ValueError, IndexError):
                    # 如果编号格式不正确，跳过这个文件夹
                    logging.warning(f'跳过格式不正确的文件夹: {item}')
                    continue
    
    # 确定新的编号
    if existing_folders:
        new_number = max(existing_folders) + 1
    else:
        new_number = 1
    
    # 创建新文件夹
    folder_name = f"session-{new_number:03d}"
    current_session_folder = os.path.join(DATA_ROOT, folder_name)
    os.makedirs(current_session_folder, exist_ok=True)
    logging.info(f'创建新会话文件夹: {folder_name}')
    return current_session_folder

# 获取当前存储路径（使用全局变量）
def get_current_folder():
    global current_session_folder
    return current_session_folder

# 获取下一个视频文件名（递增编号）
def get_next_video_filename():
    global current_video_number, current_session_folder
    # 从会话文件夹名中提取编号（如session-001提取出001）
    session_number = os.path.basename(current_session_folder).split('-')[1]
    video_name = f"ses{session_number}-{current_video_number:03d}{VIDEO_EXT}"
    current_video_number += 1
    return video_name

# 获取下一个雷达bag文件名（递增编号）
def get_next_lidar_filename():
    global current_lidar_number, current_session_folder
    # 从会话文件夹名中提取编号（如session-001提取出001）
    session_number = os.path.basename(current_session_folder).split('-')[1]
    bag_name = f"ses{session_number}-{current_lidar_number:03d}"
    current_lidar_number += 1
    return bag_name

# 视频采集线程
def video_collector():
    global video_gst_proc
    logging.info('视频采集线程启动')
    last_hour = datetime.now().hour
    while not exit_event.is_set():
        try:
            folder = get_current_folder()
            video_name = get_next_video_filename()
            video_path = os.path.join(folder, video_name)
            # 构建GStreamer命令
            gst_cmd = [
                'gst-launch-1.0',
                'udpsrc', f'address={MULTICAST_ADDR}', f'port={MULTICAST_PORT}', f'multicast-iface={INTERFACE_NAME}',
                '!',
                'application/x-rtp,media=video,encoding-name=H264',
                '!',
                'rtph264depay',
                '!',
                'h264parse',
                '!',
                'matroskamux',
                '!',
                f'filesink', f'location={video_path}'
            ]
            logging.info(f'新视频文件: {video_path}')
            # 启动GStreamer进程
            video_gst_proc = subprocess.Popen(' '.join(gst_cmd), shell=True)

            # 启动bag录制进程，文件名与视频一致加-cloud
            bag_name = video_name.replace('.mkv', '-cloud')
            bag_path = os.path.join(folder, bag_name)
            bag_cmd = [
                'ros2', 'bag', 'record',
                '-o', bag_path,
                '/utlidar/cloud_deskewed'
            ]
            bag_proc = subprocess.Popen(bag_cmd)
            logging.info(f'新bag录制: {bag_path}')

            while not exit_event.is_set():
                time.sleep(1)
                now_hour = datetime.now().hour
                if now_hour != last_hour:
                    # 用SIGINT优雅终止，保证mp4文件可播放
                    video_gst_proc.send_signal(signal.SIGINT)
                    video_gst_proc.wait()
                    logging.info(f'视频文件已关闭: {video_path}')
                    # 终止bag录制
                    bag_proc.terminate()
                    bag_proc.wait()
                    logging.info(f'bag录制结束: {bag_path}')
                    last_hour = now_hour
                    break
            # 如果是因为exit_event退出，也要优雅终止
            if exit_event.is_set():
                if video_gst_proc.poll() is None:
                    video_gst_proc.send_signal(signal.SIGINT)
                    video_gst_proc.wait()
                    logging.info(f'视频采集线程收到退出信号，已关闭: {video_path}')
                if bag_proc.poll() is None:
                    bag_proc.terminate()
                    bag_proc.wait()
                    logging.info(f'bag采集线程收到退出信号，已关闭: {bag_path}')
        except Exception as e:
            logging.error(f'视频采集线程异常: {e}')
            time.sleep(10)

# 主函数
def main():
    # 先创建会话文件夹（只在启动时创建一次）
    create_session_folder()
    
    # 然后初始化日志（使用会话文件夹名称）
    setup_logger()
    
    logging.info('自动采集脚本启动')
    # 只启动视频采集线程
    video_thread = threading.Thread(target=video_collector, daemon=True)
    video_thread.start()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info('收到中断信号，准备退出')
        exit_event.set()
        # 主动优雅终止子进程
        global video_gst_proc
        if video_gst_proc and video_gst_proc.poll() is None:
            video_gst_proc.send_signal(signal.SIGINT)
            video_gst_proc.wait()
    except Exception as e:
        logging.error(f'主循环异常: {e}')
    finally:
        logging.info('自动采集脚本已退出')

if __name__ == '__main__':
    main()
