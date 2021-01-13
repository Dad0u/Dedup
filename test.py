from dedup import Database

db = Database('test.cfg')
db.reset()
db.detect_and_add()
db.compute_video_signature()
db.compute_image_signature()
