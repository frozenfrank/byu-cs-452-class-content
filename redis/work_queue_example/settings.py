# initialize Redis connection settings
REDIS_HOST = "redis-13701.c238.us-central1-2.gce.redns.redis-cloud.com"
REDIS_PORT = 13701 # <-- likely need to change this too!
REDIS_PASSWORD = "5fugAGjn8EpMG#K"
REDIS_DB = 0

# initialize constants used to control image spatial dimensions and
# data type
IMAGE_WIDTH = 224
IMAGE_HEIGHT = 224
IMAGE_CHANS = 3
IMAGE_DTYPE = "float32"

# initialize constants used for server queuing
IMAGE_QUEUE = "image_queue"
BATCH_SIZE = 32
SERVER_SLEEP = 0.25
CLIENT_SLEEP = 0.25
