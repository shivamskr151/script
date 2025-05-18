import cv2
import subprocess
import csv
import time
import os
import sys
import threading
import logging
import signal
import atexit
from urllib.parse import urlparse

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename='stream.log', filemode='a')
logger = logging.getLogger('rtsp_to_rtmp_streamer')

# Add console handler for log output
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)

def is_rtsp_url_valid(rtsp_url):
    """Check if RTSP URL is valid by trying to connect to it"""
    try:
        cap = cv2.VideoCapture(rtsp_url)
        if not cap.isOpened():
            logger.error(f"Failed to open RTSP stream: {rtsp_url}")
            return False
        
        ret, frame = cap.read()
        cap.release()
        
        if not ret or frame is None:
            logger.error(f"Failed to read frame from RTSP stream: {rtsp_url}")
            return False
            
        logger.info(f"RTSP URL is valid: {rtsp_url}")
        return True
    except Exception as e:
        logger.error(f"Error validating RTSP URL {rtsp_url}: {str(e)}")
        return False

def stream_rtsp_to_rtmp(camera_id, rtsp_url, rtmp_url):
    """Stream from RTSP to RTMP using FFmpeg with proper H.264 handling"""
    try:
        # Create log file for this stream
        log_file = f"{camera_id}.log"
        log_handle = open(log_file, 'w')
        
        # Enhanced FFmpeg command with proper H.264 handling
        command = [
            'ffmpeg',
            '-fflags', 'nobuffer',        # Reduce latency
            '-rtsp_transport', 'tcp',      # Force TCP for RTSP
            '-i', rtsp_url,
            '-c:v', 'libx264',            # Force H.264 encoding
            '-preset', 'ultrafast',        # Minimize encoding latency
            '-tune', 'zerolatency',        # Optimize for streaming
            '-profile:v', 'baseline',      # Use baseline profile for compatibility
            '-bufsize', '2000k',           # Buffer size
            '-maxrate', '2000k',           # Maximum bitrate
            '-pix_fmt', 'yuv420p',         # Standard pixel format
            '-g', '30',                    # Keyframe interval
            '-c:a', 'aac',                 # Audio codec
            '-ar', '44100',                # Audio sample rate
            '-b:a', '128k',                # Audio bitrate
            '-f', 'flv',                   # Output format
            '-flvflags', 'no_duration_filesize',
            rtmp_url
        ]
        
        logger.info(f"Starting stream for {camera_id}: {rtsp_url} -> {rtmp_url}")
        logger.debug(f"FFmpeg command: {' '.join(command)}")
        
        # Run FFmpeg in subprocess with proper error handling
        process = subprocess.Popen(
            command,
            stdout=log_handle,
            stderr=log_handle,
            text=True,
            close_fds=True,
            bufsize=1  # Line buffered
        )
        
        # Save the log handle to close it later
        process.log_handle = log_handle
        
        # Return process so it can be terminated later if needed
        return process
        
    except Exception as e:
        logger.error(f"Error starting stream for {camera_id}: {str(e)}")
        if 'log_handle' in locals():
            log_handle.close()
        return None

def load_camera_data(csv_file='cameras.csv'):
    """Load camera data from CSV file"""
    cameras = []
    try:
        with open(csv_file, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                if len(row) >= 3:
                    camera_id = row[0]
                    rtsp_url = row[1]
                    rtmp_url = row[2]
                    cameras.append((camera_id, rtsp_url, rtmp_url))
    except Exception as e:
        logger.error(f"Error loading camera data from {csv_file}: {str(e)}")
    
    return cameras

def cleanup_processes(processes):
    """Terminate all processes"""
    logger.info("Cleaning up processes...")
    for camera_id, process in processes.items():
        try:
            process.terminate()
            # Close the log file handle
            if hasattr(process, 'log_handle'):
                process.log_handle.close()
            logger.info(f"Terminated stream for {camera_id}")
        except Exception as e:
            logger.error(f"Error terminating stream for {camera_id}: {str(e)}")

def run_as_daemon():
    """Run the script as a daemon process"""
    # Create PID file
    pid_file = 'stream.pid'
    
    # Check if process is already running
    if os.path.exists(pid_file):
        with open(pid_file, 'r') as f:
            pid = int(f.read().strip())
        try:
            # Check if the process is still running
            os.kill(pid, 0)
            logger.error(f"Process already running with PID {pid}")
            sys.exit(1)
        except OSError:
            # Process not running, remove PID file
            os.remove(pid_file)
    
    # Write PID to file
    with open(pid_file, 'w') as f:
        f.write(str(os.getpid()))
    
    # Register cleanup function
    atexit.register(lambda: os.path.exists(pid_file) and os.remove(pid_file))

def main():
    # Check if running in background mode
    daemon_mode = False
    if len(sys.argv) > 1 and sys.argv[1] == '--daemon':
        daemon_mode = True
        run_as_daemon()
        logger.info("Running in daemon mode")
    
    # Check if FFmpeg is installed
    try:
        subprocess.run(['ffmpeg', '-version'], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    except (subprocess.SubprocessError, FileNotFoundError):
        logger.error("FFmpeg is not installed or not found in PATH. Please install FFmpeg.")
        sys.exit(1)
    
    # Create CSV file if it doesn't exist
    if not os.path.exists('cameras.csv'):
        logger.info("Creating cameras.csv file with sample data")
        sample_data = [
            "camera_0005,rtsp://admin:netw9rknetw9rk@202.129.240.246:90,rtmp://34.41.186.208:1935/camera_0005/0005?username=wrakash&password=akash@1997",
            "camera_0006,rtsp://admin:netw9rknetw9rk@202.129.240.246:91,rtmp://34.41.186.208:1935/camera_0006/0006?username=wrakash&password=akash@1997",
            "camera_0007,rtsp://admin:netw9rknetw9rk@202.129.240.246:92,rtmp://34.41.186.208:1935/camera_0007/0007?username=wrakash&password=akash@1997",
            "camera_0008,rtsp://admin:netw9rknetw9rk@202.129.240.246:93,rtmp://34.41.186.208:1935/camera_0008/0008?username=wrakash&password=akash@1997"
        ]
        with open('cameras.csv', 'w') as f:
            for line in sample_data:
                f.write(line + '\n')
    
    # Load camera data
    cameras = load_camera_data()
    if not cameras:
        logger.error("No cameras found in CSV file")
        sys.exit(1)
    
    logger.info(f"Loaded {len(cameras)} cameras from CSV file")
    
    # Store processes to keep track of them
    processes = {}
    
    # Register cleanup handler for graceful exit
    def signal_handler(sig, frame):
        logger.info("Received termination signal")
        cleanup_processes(processes)
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Check RTSP URLs and start streaming
    for camera_id, rtsp_url, rtmp_url in cameras:
        logger.info(f"Checking camera {camera_id}...")
        
        # Validate RTSP URL
        if is_rtsp_url_valid(rtsp_url):
            # Start streaming
            process = stream_rtsp_to_rtmp(camera_id, rtsp_url, rtmp_url)
            if process:
                processes[camera_id] = process
        else:
            logger.warning(f"Skipping camera {camera_id} due to invalid RTSP URL")
    
    # Keep the script running and monitor streams
    try:
        while processes:
            for camera_id, process in list(processes.items()):
                # Check if process is still running
                if process.poll() is not None:
                    logger.warning(f"Stream for {camera_id} stopped with return code {process.returncode}")
                    # Close the log file handle
                    if hasattr(process, 'log_handle'):
                        process.log_handle.close()
                    
                    # Attempt to restart
                    for cam_id, rtsp_url, rtmp_url in cameras:
                        if cam_id == camera_id:
                            logger.info(f"Attempting to restart stream for {camera_id}")
                            if is_rtsp_url_valid(rtsp_url):
                                new_process = stream_rtsp_to_rtmp(camera_id, rtsp_url, rtmp_url)
                                if new_process:
                                    processes[camera_id] = new_process
                                    logger.info(f"Stream for {camera_id} restarted")
                                else:
                                    logger.error(f"Failed to restart stream for {camera_id}")
                                    del processes[camera_id]
                            else:
                                logger.error(f"RTSP URL for {camera_id} is no longer valid")
                                del processes[camera_id]
                            break
            
            # Wait before checking again
            time.sleep(10)
    except KeyboardInterrupt:
        logger.info("Stopping all streams...")
        cleanup_processes(processes)

if __name__ == "__main__":
    main()