import os,zipfile,copy,collections,re,copy,datetime


#This is an anitpattern. Should be removed as soon as all testing is done through some higher level API call
try:
    import mongoCollConn as nbaML
except ImportError:
    from . import mongoCollConn as nbaML

class nbaDbBuild(nbaML.nbaDbConnections):
    """Builds the NBA MongoDB Collection"""

    def __init__(self):
        super(nbaDbBuild,self).__init__()

    def fileReader(self,fileLocation,extension,delim):
        """Generator that reads files in location with given extension and yields a
        dictionary with the header as keys along with home/away and time fields"""
        
        for ifile in os.listdir(fileLocation):
            if not ifile.endswith(extension):
                continue

            if extension is 'zip':
                fileConn = zipfile.ZipFile(os.path.join(fileLocation,ifile),'r')
                for subFile in fileConn.namelist():

                    #pass on non-game files
                    if subFile[-1] == '/' or 'combined-stats' in subFile:
                        continue

                    #pull out the home/away team name & date from the subfile name
                    home = re.search('(?<=\d\d\d\d-\d\d/\[\d\d\d\d\-\d\d\-\d\d\]\-\d\d\d\d\d\d\d\d\d\d\-[A-Z][A-Z][A-Z]\@)[A-Z][A-Z][A-Z]',subFile).group()
                    away = re.search('(?<=\d\d\d\d-\d\d/\[\d\d\d\d\-\d\d\-\d\d\]\-\d\d\d\d\d\d\d\d\d\d\-)[A-Z][A-Z][A-Z]',subFile).group()
                    date = datetime.datetime.strptime(re.search('(?<=\d\d\d\d-\d\d/\[)\d\d\d\d\-\d\d\-\d\d',subFile).group(),'%Y-%m-%d')

                    #open the subfile, change header names from "_" delimited to camel case
                    subFileConn = fileConn.open(subFile,'r')
                    header = subFileConn.readline().decode().strip().split(delim)
                    ns = re.compile('_')
                    for index,k in enumerate(header):
                        newKey = list(k)
                        for pos in ns.finditer(k):
                            newKey[pos.start()+1] = k[pos.start()+1].upper()
                        newKey = ''.join(newKey)
                        if '_' in newKey:
                            header[index] = newKey.replace('_','')

                    #convert each record to a dictionary, add in time information from filename, then yield record
                    for line in subFileConn:
                        line = line.decode().strip().replace('.','').split(delim)
                        rec = dict(zip(header,line))
                        rec[home] = {'status':'home'}
                        rec[away] = {'status':'away'}
                        rec['date'] = date
                        rec['unixDate'] = int(date.strftime('%s'))
                        rec['status'] = {'home':home,'away':away}
                        self.rec = rec
                        yield rec
                fileConn.close()



    def insert(self,fileLocation,extension,delim):
        """Inserts data into the collection"""
        for rec in self.fileReader(fileLocation,extension,delim):
            try:
                #pass all plays where the player is blank
                if rec['player'] == '' or rec['eventType'] == 'unknown':
                    continue

                #parse the temporal data
                pl = list(map(int,rec.pop('playLength').split(':')))
                rec['playLength'] = pl[1]*60+pl[2]
                rec['season'] = rec.pop('dataSet')
                rec['period'] = int(rec['period'])
                rec['playId'] = int(rec['playId'])
                qrs = list(map(int,rec.pop('remainingTime').split(':')))
                rec['QuarterRemainingSec'] = qrs[1]*60+qrs[2]
                rec['GameRemainingSec'] = rec['QuarterRemainingSec'] + (4 - rec['period']) * 12 * 60
                rec['quarterTime'] = rec.pop('elapsed')
                rec['plyTm'] = (rec['period'] - 1) * 12 * 60 + int(rec['quarterTime'].split(':')[1]) * 60 + int(rec['quarterTime'].split(':')[2])
                rec['plyMns'] = rec['plyTm']/60
                rec['careerClock'] = rec['unixDate'] + rec['plyTm']
                rec['gameId'] = int(re.search('(?<=\"\=\"\")\d*',rec['gameId']).group())

                #parse the player data
                plyrDct = dict()
                awyPlyrs,hmPlyrs = list(),list()
                for k in ['a1', 'a2', 'a3', 'a4', 'a5', 'h1', 'h2', 'h3', 'h4', 'h5']:
                    if k[0] == 'a':
                        awyPlyrs.append(rec[k])
                    else:
                        hmPlyrs.append(rec[k])
                    del rec[k]
                for player in awyPlyrs:
                    plyrDct[player] = {'status':'away','team':rec['status']['away']}
                for player in hmPlyrs:
                    plyrDct[player] = {'status':'home','team':rec['status']['home']}
                rec[rec['status']['home']]['players'] = hmPlyrs
                rec[rec['status']['away']]['players'] = awyPlyrs
                rec['onCourt'] = hmPlyrs+awyPlyrs

                #create a game dictionary to get aggregate player time
                if 'gmDct' in locals() and gmDct['status'] == rec['status']:
                    for p in plyrDct:
                        if p not in gmDct:
                            gmDct[p] = {'lastSeen':rec['plyTm'],'secPlayed':0,'contigSecPlayed':0,'fouls':{'count':0,'reasons':{}},
                                        'freeThrow':{'taken':0,'made':0},'FGA':{'taken':0,'made':0,'type':{}},
                                        'points':0,'rebounds':{'off':0,'def':0,'total':0},'turnovers':{'count':0,'reasons':{}},
                                        'violations':{'count':0,'reasons':{}}}
                            gmDct[p].update(plyrDct[p])
                    plyrDct.update(gmDct)
                elif 'gmDct' in locals() and gmDct['status'] != rec['status']:
                    self.PBPcoll.insert(plys)
                    finalGmDct = plys[-1]
                    for k in list(finalGmDct.keys()):
                        if k not in ['season','unixDate','gameId','plyrDct','status','date','score']:
                            del finalGmDct[k]
                    self.GAMEScoll.insert(finalGmDct)
                    plys = list()
                    {plyrDct[p].update({'lastSeen':rec['plyTm'],'secPlayed':0,'contigSecPlayed':0,'fouls':{'count':0,'reasons':{}},
                                        'freeThrow':{'taken':0,'made':0},'FGA':{'taken':0,'made':0,'type':{}},
                                        'points':0,'rebounds':{'off':0,'def':0,'total':0},'turnovers':{'count':0,'reasons':{}},
                                        'violations':{'count':0,'reasons':{}}}) for p in plyrDct}
                    gmDct = copy.deepcopy(plyrDct)
                    gmDct['status'] = rec['status']
                else:
                    plys = list()
                    {plyrDct[p].update({'lastSeen':rec['plyTm'],'secPlayed':0,'contigSecPlayed':0,'fouls':{'count':0,'reasons':{}},
                                        'freeThrow':{'taken':0,'made':0},'FGA':{'taken':0,'made':0,'type':{}},
                                        'points':0,'rebounds':{'off':0,'def':0,'total':0},'turnovers':{'count':0,'reasons':{}},
                                        'violations':{'count':0,'reasons':{}}}) for p in plyrDct}
                    gmDct = copy.deepcopy(plyrDct)
                    gmDct['status'] = rec['status']
                allPlyrs = awyPlyrs+hmPlyrs
                if rec['eventType'] == 'sub':
                    allPlyrs.append(rec['left'])
                    for move in ['entered','left']:
                        gmDct[rec[move]]['lastSeen'] = rec['plyTm']
                        gmDct[rec[move]]['contigSecPlayed'] = 0
                for player in allPlyrs:
                    gmDct[player]['secPlayed'] = gmDct[player]['secPlayed'] + rec['plyTm'] - gmDct[player]['lastSeen']
                    plyrDct[player]['secPlayed'] = gmDct[player]['secPlayed']
                    plyrDct[player]['contigSecPlayed'] = plyrDct[player]['contigSecPlayed'] + rec['plyTm'] - gmDct[player]['lastSeen']
                    gmDct[player]['lastSeen'] = rec['plyTm']

                #clean the record up
                rec['off'] = rec.pop('team')
                rec['def'] = [j for j in rec['status'].values() if j != rec['off']][0]
                kdels = list()
                del rec['home']
                del rec['away']
                rec['Score'] = {rec['status']['home']:int(rec.pop('homeScore')),
                                rec['status']['away']:int(rec.pop('awayScore'))}

                if rec['player'] not in plyrDct.keys():
                    print(rec)
                    continue
                
                #blocks, steals, assists
                for playType in ['assist','block','steal']:
                    if playType in rec.keys() and rec[playType] != '':
                        try:
                            plyrDct[rec[playType]][playType] += 1
                        except KeyError:
                            plyrDct[rec[playType]][playType] = 1


                #cumulate points
                if 'points' in rec.keys() and rec['points'] != '':
                    rec['points'] = int(rec['points'])
                    plyrDct[rec['player']]['points'] += int(rec['points'])
                
                #free throws
                if rec['eventType'] == 'free throw':
                    plyrDct[rec['player']]['freeThrow']['taken'] += 1
                    if rec['result'] == 'made':
                        plyrDct[rec['player']]['freeThrow']['made'] += 1

                #shots
                if rec['eventType'] in ['shot','miss']:
                    plyrDct[rec['player']]['FGA']['taken'] += 1
                    rec['shotType'] = '2PT'
                    if re.search('3PT',rec['description']):
                        rec['shotType'] = '3PT'
                        try:
                            plyrDct[rec['player']]['FGA']['type']['3PT']['taken'] += 1
                        except KeyError:
                            try:
                                plyrDct[rec['player']]['FGA']['type']['3PT']['taken'] = 1
                            except KeyError:
                                plyrDct[rec['player']]['FGA']['type']['3PT'] = {'taken':1}
                    else:
                        rec['shotType'] = '2PT'
                        try:
                            plyrDct[rec['player']]['FGA']['type']['2PT']['taken'] += 1
                        except KeyError:
                            try:
                                plyrDct[rec['player']]['FGA']['type']['2PT']['taken'] = 1
                            except KeyError:
                                plyrDct[rec['player']]['FGA']['type']['2PT'] = {'taken':1}
                    try:
                        plyrDct[rec['player']]['FGA']['type'][rec['type']]['taken'] += 1
                    except KeyError:
                        try:
                            plyrDct[rec['player']]['FGA']['type'][rec['type']]['taken'] = 1
                        except KeyError:
                            plyrDct[rec['player']]['FGA']['type'][rec['type']] = {'taken':1}
                    if rec['eventType'] == 'shot':
                        plyrDct[rec['player']]['FGA']['made'] += 1
                        if re.search('3PT',rec['description']):
                            try:
                                plyrDct[rec['player']]['FGA']['type']['3PT']['made'] += 1
                            except KeyError:
                                try:
                                    plyrDct[rec['player']]['FGA']['type']['3PT']['made'] = 1
                                except KeyError:
                                    plyrDct[rec['player']]['FGA']['type']['3PT'] = {'made':1}
                        else:
                            try:
                                plyrDct[rec['player']]['FGA']['type']['2PT']['made'] += 1
                            except KeyError:
                                try:
                                    plyrDct[rec['player']]['FGA']['type']['2PT']['made'] = 1
                                except KeyError:
                                    plyrDct[rec['player']]['FGA']['type']['2PT'] = {'made':1}
                        try:
                            plyrDct[rec['player']]['FGA']['type'][rec['type']]['made'] += 1
                        except KeyError:
                            try:
                                plyrDct[rec['player']]['FGA']['type'][rec['type']]['made'] = 1
                            except KeyError:
                                plyrDct[rec['player']]['FGA']['type'][rec['type']] = {'made':1}
                                
                #rebounds
                if rec['type'] == 'rebound offensive':
                    plyrDct[rec['player']]['rebounds']['off'] += 1
                    plyrDct[rec['player']]['rebounds']['total'] += 1
                elif rec['type'] == 'rebound defensive':
                    plyrDct[rec['player']]['rebounds']['def'] += 1
                    plyrDct[rec['player']]['rebounds']['total'] += 1

                #turnovers
                if rec['eventType'] == 'turnover':
                    plyrDct[rec['player']]['turnovers']['count'] += 1
                    try:
                        plyrDct[rec['player']]['turnovers']['reasons'][rec['reason']] += 1
                    except KeyError:
                        plyrDct[rec['player']]['turnovers']['reasons'][rec['reason']] = 1

                #violations
                if rec['eventType'] == 'violation':
                    plyrDct[rec['player']]['violations']['count'] += 1
                    try:
                        plyrDct[rec['player']]['violations']['reasons'][re.search('(?<=violation:)[a-z\s]*',rec['type']).group()] += 1
                    except KeyError:
                        plyrDct[rec['player']]['violations']['reasons'][re.search('(?<=violation:)[a-z\s]*',rec['type']).group()] = 1

                #fouls
                if rec['eventType'] == 'foul':
                    plyrDct[rec['player']]['fouls']['count'] += 1
                    try:
                        plyrDct[rec['player']]['fouls']['reasons'][rec['type']] += 1
                    except KeyError:
                        plyrDct[rec['player']]['fouls']['reasons'][rec['type']] = 1

                #ejections
                if rec['eventType'] == 'ejection':
                    plyrDct[rec['player']]['ejection'] = rec['type']

                #embed player dict inside the 
                rec['plyrDct'] = plyrDct

                #clean out the blank KV pairs
                for k,v in list(rec.items()):
                    if v in [None,'']:
                        del rec[k]

                plys.append(rec)
            except:
                self.ERRcoll.insert(rec)


    def archive(self,archiveFile,archiveLocation):
        """Archives files"""
        os.rename(archiveFile,os.path.join(archiveFile,archiveLocation))

    def makeIndex(self):
        self.PBPcoll.create_index([("player",nbaML.pymongo.ASCENDING),("eventType",nbaML.pymongo.ASCENDING),("type",nbaML.pymongo.ASCENDING)])
        self.PBPcoll.create_index("def")
        self.PBPcoll.create_index("off")
        self.PBPcoll.create_index("eventType")
        self.PBPcoll.create_index("steal",sparse=True)
        self.PBPcoll.create_index("assist",sparse=True)
        self.PBPcoll.create_index("block",sparse=True)
        self.PBPcoll.create_index("onCourt")

test = nbaDbBuild()
test.insert(fileLocation='/Users/colinusala/Downloads/nbaPBP',extension='zip',delim=',')
test.makeIndex()
