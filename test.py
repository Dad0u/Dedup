from dedup import Database

db = Database('test.cfg')
db.reset()
db.detect_and_add()
