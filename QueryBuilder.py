"""
This module is an API to the nba database. This is imported into the view portion of the django nba app.
The Query Runner class is instantiated in views.py and then URLs are routed to QueryRunner methods 
depending on what the user is trying to pull.
"""


#This is an anitpattern. Should be removed as soon as all testing is done through views.py
try:
    import mongoCollConn as nbaML
except ImportError:
    from . import mongoCollConn as nbaML


# from . import mongoCollConn as nbaML
import copy


def sdc(*args):
    """Simple concatenation of arguments, returns a string joined by a period to access MongoDB sub-documents"""
    return ".".join(list(args))

def eqCheck(field,value):
    return {"$eq":['$'+field,value]}

def condCheck(tReturn,fReturn,conj=None,*args):

    if len(args > 1) and not conj:
        raise Exception('must pass a conjunction if giving more than one condition')

    if conj:
        return {"$cond":[{conj:args},tReturn,fReturn]}

    return {"$cond":[args[0],tReturn,fReturn]}

def aggMatches(condCheck,incValue=1):
    return {metric:{"$sum":{condCheck}}}


def perUnitCalc(record,unit,aggMetrics):
    """
    Takes a MongoDB record of player/team summary stats, and transforms into a per game, per 36 min, etc.
    Input is a record, the unit scaled by, and a list of metrics to condition on 
    (not everything int he record will be a metric to scale).
    If scaling is per game, assumes that the games played metric is pulled in the record passed to perUnitCalc.
    """
    retRecord = copy.deepcopy(record)
    for record in retRecord:
        for metric in record.keys():
            if metric in aggMetrics and metric != 'SECPLAYED':
                if unit == 'game':
                    record[metric] = float(record[metric])/record.get('GP')
                elif unit == '36Min':
                    record[metric] = float(record[metric])/(float(record.get('SECPLAYED'))/(60*36))
    return retRecord


#add filter for all of these by season/post season
#add in plus/minus status
class QueryRunner(object):
    """Stores the Queries and Database Connections. Queries are stored in methods, DB connections are class attributes."""
    #connections
    nbaPBP = nbaML.nbaDbConnections()
    PBPcoll = nbaPBP.PBPcoll
    GMcoll = nbaPBP.GAMEScoll
    def __init__(self):
        pass

    def courtChart(self,playType,player=None,offTeam=None,defTeam=None,
                       otherPlayers=None,seasons=None,dateMin=None,dateMax=None,
                       originalXY=True,generator=None,returnType='listOfDict'):
        """
        Returns a list of or a generator of tuples or dictionary of shot details.

        'playType' specifies whether it returns: shots, makes, misses, blocked, assisted, blocks, assists
        'player' specifies which player the short chart is returned for
        'offTeam' specifies which team was on offense for a given play
        'defTeam' specifies which team was on defense for a given play
        'otherPlayer' list specifying players other than primary that are on court during play
        'team' to filter results by a particular team
        'dateMin/dateMax' is a unix timestamp filter to cut points to a date range
        'seasons' list of seasons that filters points to only include those seasons
        'originalXY' boolean specifying using original or converted xy coordinates, defaults to original
        'generator' boolean to return a generator, defaults to object
        'returnType' can return either a list of dictionaries, or a list of tupes with structure:
            tuple order: xLoc,yLoc,eventType,type,player,offteam,defteam
            dict keys are: "eventType","originalY","originalX","type","player","off","def"
        
        """

        #determine if want the original x,y corrdinates, or converted x,y coordinates
        if originalXY:
            xtype,ytype = 'originalX','originalY'
        else:
            xtype,ytype = 'convertedX','convertedY'

        #parameterize the query and build the cursor
        query = {}
        if player:
            query['player'] = player
        if playType == 'shots':
            query['$or'] = [{'eventType':'shot'},{'eventType':'miss'}]

        elif playType == 'makes':
            query['eventType'] = 'shot'

        elif playType == 'blocks':
            query['eventType'] = 'miss'

        elif playType == 'blocked':
            query['block'] = {'$exists':True}

        elif playType == 'blocks':
            query['block'] = player
            del query['player']

        elif playType == 'assisted':
            query['assist'] = {'$exists':True}

        elif playType == 'assists':
            query['assist'] = player
            del query['player']

        if offTeam:
            query['off'] = offTeam
        elif defTeam:
            query['def'] = defTeam

        if otherPlayers:
            query['onCourt'] = {'$in':otherPlayers}

        if seasons:
            query['season'] = {'$in':seasons}

        if dateMin:
            query['unixDate'] = {'$gt':dateMin}
        if dateMax:
            if 'unixDate' in query:
                query['unixDate'].update({'$lt':dateMax})
            else:
                query['unixDate'] = {'$lt':dateMax}

        queryCursor = QueryRunner.PBPcoll.find(query,{xtype:True,ytype:True,'eventType':True,
                                                        'type':True,'off':True,'def':True,'player':True,'_id':False})

        #return a list or a generator of the values
        if returnType == 'listOfTuples':
            rval = map(lambda x: (x.get(xtype),x.get(ytype),x.get('eventType'),x.get('type'),x.get('player'),x.get('off'),x.get('def')),queryCursor)
        elif returnType == 'listOfDict':
            rval = queryCursor
        if generator:
            return rval
        else:
            return list(rval)

##    def splits(self,metrics=[],player=None,team=None,onCourt=[],splits=[]):
##        ## Need to add in a count of all metrics when player on court, also must add in count of total number of plays
##        ## Need to add in a filter functionality
##        ## Need to add in a not on court parameter as well
##        """
##        takes a player or team and aggregates metrics split on some list of parameters. Here split acts as _id in mongo doc.
##
##        may specify team or other players onCourt for filters. onCourt acts as an $and operator in mongo
##        
##        metrics for aggregation: 
##        points, fga, makes, misses, fgPercent, 2Pa, 2PaMakes, 2PaPercent, 3Pa, 3PaMakes, 3PaPercent,
##        fta, ftMakes, ftPercent, reb, offReb, defReb, rebPerc, offRebPerc, defRebPerc, stl, tov, pf, 
##        violations, ejections, assists, blocks, assitedShots, blockedShots, 
##        percentShotsAssisted, percentofTeamShotsAssisted, percentShotsBlocked, 
##        percentOfOpponentShotsBlocked, percentOfTeamShotsBlocked,tovAsstRatio
##        """
##        pipeline = []
##
##        grpStmnt = {'$group':{'_id':{}}}
##        aggStmnt = {'$sum':1}
##
##        eventTypeMetrics = ['miss','shot','rebound','turnover','foul','timeout',
##                        'violation','sub','jump ball','end of period','free throw','ejection']
##
##        ownKeyMetrics = ['assist','block','steal']
##
##        specialMetrics = {'2Pa':aggMatches(condCheck(1,0,None,eqCheck('shotType','2PT'))),
##        '2PaMakes':aggMatches(condCheck(1,0,'and',eqCheck('shotType','2PT'),eqCheck('eventType','shot'))),
##        '3Pa':aggMatches(condCheck(1,0,None,eqCheck('shotType','3PT'))),
##        '3PaMakes':aggMatches(condCheck(1,0,'and',eqCheck('shotType','3PT'),eqCheck('eventType','shot'))),
###########
#### finish off the metrics that have special aggregations for $group statements
###########
##        'fta','ftMakes',
##                    'reb','offReb','defReb','stl','tov','pf','violations','ejections',
##                    'assistedShots','blockedShots'}
##
##        perMetrics = {'fgPerc':('makes','fga'),
##                    '2PaPerc':('2PaMakes','2Pa'),
##                    '3PaPerc':('3PaMakes','3Pa'),
##                    'ftPerc':('fta','ftMakes'),
##                    'rebPerc':('reb','misses')}
##
##        if player or team or len(onCourt) >= 1:
##            pipeline.insert(0,{'$match':{}})
##        if player:
##            pipeline[0]['$match'].update({"player":player})
##        if team:
##            pipeline[0]['$match'].update({team:{"$exists":True}})
##        for onCourtplayer in onCourt+player:
##            try:
##                pipeline[0]['$match']['$and'].append({'onCourt':onCourtplayer})
##            except KeyError:
##                pipeline[0]['$match'].update({'$and':[{'onCourt':onCourtplayer}]})
##
##        for metric in metrics:
##            if metric in eventMetrics:
##                grpStmnt['$group'].update({metric:{"$sum":{"$cond":[{"$eq":["$eventType",metric]},1,0]}}})
##            elif metric in nonEventMetrics:
##                pipeline[0] = {'$match':{'$or':[pipeline[0]['$match'],{metric:player}]}}
##                grpStmnt['$group'].update({metric:{"$sum":{"$cond":[{"$eq":["$"+metric,player]},1,0]}}})
##            
##        for split in splits:
##            grpStmnt['$group']['_id'].update({split:'$'+split})
##
##        grpStmnt['$group'].update({'numPlays':{'$sum':1}})
##        pipeline.append(grpStmnt)
##        queryCursor = QueryRunner.PBPcoll.aggregate(pipeline)
##        qR = queryCursor.get('result')
##        res = [rec for rec in qR if len(set(rec['_id'].keys()).intersection(set(splits)))== len(splits)]
##        return queryCursor.get('result')

    def gamesPlayed(self,player=None,team=None,playerTeam=None,homeAway=None,generator=None):
        """
        Returns a list of a team's or player's or a specific player/team combo's gameIds 
        May use home/Away to filter for whether team is home or away
        Best for using in aggregation pipelines for +/- calculations
        """
        query = {}
        plyrSubDoc = sdc('plyrDct',player)

        if player:
            query.update({plyrSubDoc:{'$exists':1}})
        if team:
            query.update({'$or':[{'status.away':team},{'status.home':team}]})
        if playerTeam:
            query[sdc(plyrSubDoc,'team')] = playerTeam
            del query[sdc('plyrDct',player)]
        if homeAway:
            if team:
                query.update({sdc('status',homeAway):team})
            elif player:
                query.update({sdc(plyrSubDoc,'status'):homeAway})
        queryCursor = QueryRunner.GMcoll.find(query,{'_id':0,'gameId':1})

        genne = map(lambda x: x.get('gameId'),queryCursor)
        if generator:
            return genne
        else:
            return list(genne)


    def playerGameStats(self,player,metrics='all',splits='all',per=['game'],gp=True):
        """
        Aggregates player statitics split on status, season, team in _id field.

        Takes a player as an argument for determining who to pull for.

        Will also calculate per: game, 36min, etc. based on per=[per unit basis list]

        gp argument is true/false, will return number of games played for some filtered set.
        """

        #placeholder for agg pipeline
        aggpln = [{'$match':{sdc('plyrDct',player):{'$exists':True}}},{'$group':{}}]

        #setting up the metric portion of the aggregation pipeline

        aggMetrics = {
        'FGA':'FGA.taken',
        'FGM':'FGA.made',
        'PTS':'points',
        '2PTS':'FGA.type.2PT.made',
        '2PA':'FGA.type.2PT.taken',
        '3PTS':'FGA.type.3PT.made',
        '3PA':'FGA.type.3PT.taken',
        'FTA':'freeThrow.taken',
        'FTM':'freeThrow.made',
        'DRB':'rebounds.def',
        'ORB':'rebounds.off',
        'TRB':'rebounds.total',
        'STL':'steal',
        'AST':'assist',
        'FOUL':'fouls,',
        'SECPLAYED':'secPlayed',
        'TOV':'turnovers.count',
        'VIO':'violations.count',
        'BLKS':'block'
        }

        if metrics == 'all':
            metrics = aggMetrics.keys()

        qryMetrics = {}

        for metric in metrics:
            if metric in aggMetrics:
                qryMetrics.update({metric:{'$sum':sdc('$plyrDct',player,aggMetrics[metric])}})

        if gp:
            qryMetrics.update({'GP':{'$sum':1}})

        #setting up the split portion of the aggregation pipeline

        spltsDct = {
        'status':sdc('$plyrDct',player,'status'),
        'season':'$season',
        'team':sdc('$plyrDct',player,'team')
        }

        if splits == 'all':
            splits = spltsDct.keys()

        qrySplts = {'_id':{}}

        for split in splits:
            qrySplts['_id'].update({split:spltsDct[split]})

        {aggpln[1]['$group'].update(y) for y in [qryMetrics,qrySplts]}

        queryCursor = QueryRunner.GMcoll.aggregate(aggpln)

        qR = queryCursor.get('result')

        if 'PG' in per:
            pgQuery = perUnitCalc(record=qR,unit='PG',aggMetrics=metrics)

        if '36Min' in per:
            thirtySixMinQuery = perUnitCalc(record=qR,unit='36Min',aggMetrics=metrics)

        if metrics == aggMetrics.keys() or 'FG%' in metrics and 'FT%' in metrics:
            map(lambda x: x.update({
                'FG%':float(x.get('FGM'))/x.get('FGA'),
                'FT%':float(x.get('FTM'))/x.get('FTA'),
                '2PT%':float(x.get('2PTS'))/x.get('2PA'),
                '3PT%':float(x.get('3PTS'))/x.get('3PA')
                }), qR)

        returnQuery = {'totals':qR}
        if 'pgQuery' in locals():
            returnQuery['pg'] = pgQuery
        if 'thirtySixMinQuery' in locals():
            returnQuery['36Min'] = thirtySixMinQuery

        return returnQuery

    def shotDeepDive(self,player,splits='all'):
        """
        Shot deep dive is for a player
        Can split on status, season, team shotType, shotDistance
        """

        aggpln = [{'$match':{'player':player,'eventType':{'$in':['shot','miss']}}}]

        spltsDct = {
        'status':sdc('$plyrDct',player,'status'),
        'season':'$season',
        'team':sdc('$plyrDct',player,'team'),
        'shotType':'$shotType',
        'shotDistance':'$shotDistance'
        }

        if splits == 'all':
            splits = spltsDct
        else:
            splits = {split:spltsDct[split] for split in splits}

        grpStmnt = {'$group':{'_id':splits,'count':{'$sum':1}}}

        for metric in ['shot','miss']:
            grpStmnt['$group'].update({metric:{"$sum":{"$cond":[{"$eq":["$eventType",metric]},1,0]}}})

        aggpln.append(grpStmnt)

        queryCursor = QueryRunner.PBPcoll.aggregate(aggpln)

        return queryCursor.get('result')

    def plusMinus(self,player,splits='all'):

        spltsDct = {
        'status':sdc('$plyrDct',player,'status'),
        'season':'$season',
        'team':sdc('$plyrDct',player,'team')
        }
        if splits == 'all':
            splits = spltsDct
        else:
            splits = {split:spltsDct[split] for split in splits}

        match = {'$match':{'onCourt':player}}
        aggStmnt = {'plus':{'$sum':{'$cond':[{'$eq':[sdc('$plyrDct',player,'team'),'$off']},'$points',{'$multiply':[-1,'$points']}]}}}
        group = {'$group':{'_id':splits}}
        group['$group'].update(aggStmnt)

        aggpln = [match,group]

        queryCursor = QueryRunner.PBPcoll.aggregate(aggpln)

        return queryCursor.get('result')

