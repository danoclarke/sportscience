import pymongo

class nbaDbConnections(object):
    """Builds the NBA MongoDB Collection"""

    def __init__(self,loc='mongodb://localhost'):
       conn = pymongo.MongoClient(loc)
       db = conn.nba_all
       self.PBPcoll = db.playbyplay
       self.GAMEScoll = db.games
       self.ERRcoll = db.err       
