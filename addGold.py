# coding=utf-8
# edit by zhangyi  for test
# 批量加金币
from config import *
import logging.handlers

# fab -w -z 100 --hide=commands,warnings --no-pty -f addGold.py doselectSql:servers=1-2042
uidServerDict = {}
uidDict = {}
LOG_FILE = 'tst.log'

handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes=1024 * 1024 * 1024, backupCount=5)
fmt = '%(message)s'

formatter = logging.Formatter(fmt)
handler.setFormatter(formatter)

logger = logging.getLogger('tst')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


@task
@runs_once
def doselectSql(prodType="cok-prod", servers="all"):  # 默认全服执行
    global uidDict, uid, uidstr
    global uidServerDict
    uids = []
    fd = open("gold.txt")
    uidstr = ""
    for line in fd:
        line = line.strip('\r\n')
        num = line.split(' ')[1]
        # itemId = line.split(' ')[2]
        uid = line.split(' ')[0]
        # zoneid = int(line.split(' ')[0])
        # print("zoneid=%s") %(zoneid)
        if not uidDict.has_key(uid):
            uidDict[uid] = []
        uidDict[uid].append({"num": num})
        #
        # if not uidServerDict.has_key(zoneid):
        #     uidServerDict[zoneid] = []
        # uids.append(uid)
        # if uid not in uidServerDict[zoneid]:
        #     uidServerDict[zoneid].append(uid)
        uidstr = uidstr + "'" + uid + "'" + ","
    finaluidstr = uidstr[:-1]
    print finaluidstr
    mCon = None
    mCur = None
    try:
        mCon = MySQLdb.Connect(host='10.142.9.26', user="root", passwd="t9qUzJh1uICZkA",
                               db='cokdb_global', port=8066, connect_timeout=30)
        mCur = mCon.cursor()
        sqlstr = "select gameUid,server from account_new where gameUid in (%s)" % finaluidstr
        mCur.execute(sqlstr)
        results = mCur.fetchall()
        for res in results:
            uid = res[0]
            zoneid = res[1]
            if not uidServerDict.has_key(zoneid):
                uidServerDict[zoneid] = []
            uidServerDict[zoneid].append(uid)
    except:
        print str
    # print uidServerDict
    setHosts(prodType, servers)  # 将要处理的服务器信息放入env.host
    execute(setSelectSql)
    print "done!"


@task
@parallel
def setSelectSql():
    global uidDict
    global uidServerDict
    # print uidServerDict
    servers = host_dict[env.host]  # 通过env.host 获得服务器id列表
    # print(servers)
    for server in servers:
        # print server
        sInfo = server_dict[server]
        if not uidServerDict.has_key(server):
            # print uidServerDict
            continue
        uids = uidServerDict[server]
        print uids
        mCon = None
        mCur = None
        try:
            # print "come in !"
            mCon = MySQLdb.Connect(host=sInfo['inDbIp'], user='root', passwd='t9qUzJh1uICZkA', db=sInfo['dbName'],
                                   port=3306, connect_timeout=30)
            mCur = mCon.cursor()
            for uid in uids:
                # 踢人
                shell = "curl -s http://127.0.0.1:8080/gameservice/kickuserforbackdata?uid=%s\&zoneId=%s" % (
                    uid, server)
                print shell
                run(shell)
                golds = uidDict[uid]
                for gold in golds:
                    sql1 = 'select `gold` from userprofile where uid="%s" ' % (uid)
                    mCur.execute(sql1)
                    r1 = mCur.fetchall()
                    # for r in r1:
                    count = long(r1[0][0])
                    # owe = count - long(reward['num'])

                    # if owe < 0:
                    #     owe = 0
                    logger.info("%s  %s  %d  %d  " % (server, uid, count, (long)(gold['num'])))
                    sql2 = 'update userprofile set gold = gold + %d where uid="%s"' % (
                        (long)(gold['num']), uid)
                    print(sql2)
                    mCur.execute(sql2)
                    mCon.commit()
                    logger.info("succ  %s  %s  %d  %d  " % (server, uid, count, (int)(gold['num'])))
                    # sqlStr = 'select ownerId,itemId from user_item where ownerId="%s" and itemId="%s" '%(uid,reward['itemId'])
                    # mCur.execute(sqlStr)
                    # results = mCur.fetchall()
                    # for result in results:
                    #     ownId = result[0]
                    #     itemId = result[1]

        except MySQLdb.Error, e:
            logger.info("Mysql Error %d: %s  ,%s   %s" % (
                e.args[0], e.args[1], server, uid))
        finally:
            if mCur:
                mCur.close()
            if mCon:
                mCon.close()
    return ""
