import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path
import subprocess
import datetime
class ManicTimeLoader():
    def __init__(self,db_path=None,mtc_path=None):
        if not db_path:
            self.db_path = "sqlite:///{}/AppData/Local/Finkit/ManicTime/ManicTimeReports.db"\
                            .format(str(Path.home()).replace("\\","/"))
        self.engine = create_engine(self.db_path)

        if not mtc_path:
            self.mtc_path = "C:/Program Files (x86)/ManicTime"

    def get_mtc(self,schema,from_date=None,to_date=None,save_dir=None, save_csv=False):
        if(schema not in ["ComputerUsage","Applications","Documents"]):
            print("Failed")
            return
        
        # コマンドの生成
        if(save_dir==None):
            file_name = "ManicTime_"+schema+"_"+datetime.datetime.now().strftime('%Y%m%d%H%M%S')+".csv"
            save_dir = str(self.mtc_path/file_name)
        cmd = ["mtc","export","ManicTime/"+schema,save_dir]
        if(from_date):
            cmd.append("/fd:"+from_date)
        if(to_date):
            cmd.append("/td:"+to_date)    
        
        # コマンドの実行
        print(" ".join(cmd))
        proc = subprocess.run(cmd, cwd=self.mtc_path, shell=True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        print(proc.stdout.decode("cp932"))
        
        if(save_csv): # CSVを保存したまま終了
            return
        else: # CSVは削除してDataFrameを返す
            r_df = pd.read_csv(save_dir,dtype=str)
            r_df["Start"] = pd.to_datetime(r_df["Start"])
            r_df["End"] = pd.to_datetime(r_df["End"])
            r_df["Duration"] = pd.to_timedelta(r_df["Duration"])
            subprocess.run(["del",save_dir], shell=True, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            return r_df


    def get_query(self,schema,from_date=None,to_date=None):
        
        # 抽出期間
        localtime = ""
        if(to_date):
            localtime += " and a.StartLocalTime < '{}' ".format(to_date)
        if(from_date):
            localtime += " and a.EndLocalTime > '{}' ".format(from_date)
        
        # スキーマの列
        if(schema=="Applications"):
            cols = ["a.CommonGroupId AppId,",   "cg.Name AppName,","a.Name AppDetail"]
        elif(schema=="Documents"):
            cols = ["a.CommonGroupId DomainId,","cg.Name Domain,", "a.Name URL"      ]
        elif(schema=="ComputerUsage"):
            cols = [""                          ,""              , "a.Name Status"   ]
        
        # CoputerUsage以外では必要
        commonid = "join Ar_CommonGroup cg on a.CommonGroupId = cg.CommonId" if(schema!="ComputerUsage") else ""
        
        arg_str = cols+[commonid,schema,localtime]
        
        query = """
        select 
            a.StartLocalTime, 
            a.EndLocalTime,
            {0[0]}
            {0[1]}
            {0[2]}
        from Ar_Activity a
        {0[3]}
        join Ar_Timeline t on a.ReportId = t.ReportId
        where 
            t.SchemaName = 'ManicTime/{0[4]}'
            {0[5]}
        """.format(arg_str)
        return query

    def read_db(self,schema,from_date=None,to_date=None):
        query = self.get_query(schema,from_date,to_date)
        df = pd.read_sql(query,self.engine,parse_dates=["StartLocalTime","EndLocalTime"])
        return df

    
