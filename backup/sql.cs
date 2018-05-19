using System;
using System.Collections.Generic;
using System.Text;
using MySql.Data.MySqlClient;
using System.IO;
using System.Data;

namespace backupCommand
{
    public class ID_Table
    {
        public int histroy_id;
        public string table_name;
    }
    public class SqlClass
    {
        private readonly int CACHE_SIZE = 20;
        private MySqlConnection conn;
        private static string ConnectionString;
        private static SqlClass instance;
        private static Dictionary<string, string> file_name_dict = new Dictionary<string, string>();
        private static Dictionary<string, string> path_dict = new Dictionary<string, string>();
        private static List<string> exist_md5 = new List<string>();
        public static string SERVER_HOST;
        public static string DATABASE_NAME;
        public static string SQL_USER;
        public static string SQL_PASSWORD;
        private List<string> InsertCache = new List<string>();
        public static readonly object connectionLocker = new object();
        public static readonly object fileNameLocker = new object();
        public static readonly object folderLocker = new object();
        public static readonly object cacheLocker = new object();
        private string currentBackupTable;

        private SqlClass()
        {
            ConnectionString = string.Format(@"server={0};port=3306;user id={1};password={2};database={3};allow zero datetime=true", SERVER_HOST, SQL_USER, SQL_PASSWORD, DATABASE_NAME);
            this.conn = new MySqlConnection(ConnectionString);
        }

        public void Init()
        {
            int startIndex = 0;
            int SELECT_COUNT_PER = 30000;

            currentBackupTable = "backup_" + DateTime.Now.ToString("yyyyMM");

            MySqlDataAdapter adp = new MySqlDataAdapter();
            DataTable dt = new DataTable();

            MySqlCommand createCommand = conn.CreateCommand();
            createCommand.CommandText = string.Format(@"CREATE TABLE IF NOT EXISTS `{0}` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `backup_date` datetime DEFAULT NULL,
  `filepath_id` int(11) DEFAULT NULL,
  `filename_id` int(11) DEFAULT NULL,
  `modifydate` datetime DEFAULT NULL,
  `md5` varchar(45) DEFAULT NULL,
  `histroy_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;", currentBackupTable);
            conn.Open();
            createCommand.ExecuteNonQuery();
            conn.Close();


            MySqlCommand selectCommand = conn.CreateCommand();
            while (true)
            {
                selectCommand.CommandText = string.Format("select id,path from tb_path limit {0}, {1}", startIndex, SELECT_COUNT_PER);
                adp.SelectCommand = selectCommand;
                adp.Fill(dt);

                if (dt.Rows.Count == 0) break;

                foreach (DataRow row in dt.Rows)
                {
                    if (!path_dict.ContainsKey(row["path"].ToString()))
                        path_dict.Add(row["path"].ToString(), row["id"].ToString());
                }
                dt.Clear();
                startIndex += SELECT_COUNT_PER;
            }
            dt.Clear();

            startIndex = 0;
            while (true)
            {
                selectCommand.CommandText = string.Format("select id,filename from filename limit {0}, {1}",startIndex, SELECT_COUNT_PER);
                adp.SelectCommand = selectCommand;
                adp.Fill(dt);

                if (dt.Rows.Count == 0) break;
                foreach (DataRow row in dt.Rows)
                {
                    if (!file_name_dict.ContainsKey(row["filename"].ToString()))
                        file_name_dict.Add(row["filename"].ToString(), row["id"].ToString());
                }
                dt.Clear();
                startIndex += SELECT_COUNT_PER;
            }
            dt.Clear();

            startIndex = 0;
            while (true)
            {
                selectCommand.CommandText = string.Format("select md5 from md5 limit {0}, {1}", startIndex, SELECT_COUNT_PER);
                adp.SelectCommand = selectCommand;
                adp.Fill(dt);

                if (dt.Rows.Count == 0) break;

                foreach (DataRow row in dt.Rows)
                {
                    exist_md5.Add(row["md5"].ToString());
                }
                dt.Clear();
                startIndex += SELECT_COUNT_PER;
            }
        }
        public static SqlClass GetInstance()
        {
            if (instance == null)
            {
                instance = new SqlClass();
            }
            return instance;
        }

        public string GetNewBackupHistroyId(string folderId)
        {
            MySqlConnection mConn = new MySqlConnection(ConnectionString);
            try
            {
                mConn.Open();
                MySqlCommand sqlcmd = mConn.CreateCommand();
                sqlcmd.CommandText = string.Format(@"INSERT INTO `filebackupsys`.`histroy` (`backup_date`,`path_id`, `backup_table`) VALUES ('{0}',{1},'{2}');",
                    DateTime.Now.ToString("yyyy-MM-dd  HH-mm-ss"), folderId, currentBackupTable);

                sqlcmd.ExecuteNonQuery();
                
                return sqlcmd.LastInsertedId.ToString();
            }
            catch (Exception e)
            {
                throw e;
            }
            finally
            {
                if (mConn.State == ConnectionState.Open)
                {
                    mConn.Close();
                }
            }
        }

        public string GetFileNameId(string filename)
        {
            lock (fileNameLocker)
            {
                if (file_name_dict.ContainsKey(filename))
                {
                    return file_name_dict[filename];
                }
                else
                {
                    MySqlConnection mConn = new MySqlConnection(ConnectionString);
                    try
                    {
                        mConn.Open();
                        MySqlCommand sqlcmd = mConn.CreateCommand();
                        sqlcmd.CommandText = string.Format(@"INSERT INTO `filename` (`filename`) values('{0}')", filename.Replace("'", "\\'"));

                        sqlcmd.ExecuteNonQuery();
                        file_name_dict.Add(filename, sqlcmd.LastInsertedId.ToString());
                        return sqlcmd.LastInsertedId.ToString();

                    }
                    catch (Exception e)
                    {
                        throw e;
                    }
                    finally
                    {
                        if (mConn.State == ConnectionState.Open)
                        {
                            mConn.Close();
                        }
                    }
                }
            }
        }


        private void Conn_StateChange(object sender, StateChangeEventArgs e)
        {
            throw new NotImplementedException();
        }

        public void AddMd5(string md5)
        {

            MySqlConnection mConn = new MySqlConnection(ConnectionString);
            try
            {
                mConn.Open();

                MySqlCommand sqlcmd = mConn.CreateCommand();

                sqlcmd.CommandText = String.Format("INSERT INTO `md5` (`md5`) VALUES ('{0}')", md5);

                sqlcmd.ExecuteNonQuery();

                exist_md5.Add(md5);
            }
            catch (Exception err)
            {
                throw err;
            }
            finally
            {
                if (mConn.State == ConnectionState.Open)
                {
                    mConn.Close();
                }
            }
        }

        public DataTable GetFileList(int histroyId, int pathId,string backupTable)
        {

            DataTable table = new DataTable();
            try
            {
                string selectCommand = string.Format(@"select t1.id as id, t1.filename as filename,t2.modifydate as modifydate, t2.md5 as md5 from filename as t1 right join 
(select filename_id,modifydate,md5 from {2} where histroy_id = {0} and filepath_id = {1}) as t2
on t2.filename_id = t1.id", histroyId, pathId, backupTable);
                Console.WriteLine(selectCommand);

                MySqlDataAdapter adp = new MySqlDataAdapter(selectCommand, conn);

                adp.Fill(table);
            }
            catch (Exception e)
            {
                throw e;
            }
            return table;
        }

        public void Insert(FileInfo fi, string md5, string filePathId,string histroyId)
        {
            try
            {
                string fileNameId = GetFileNameId(fi.Name);

                string now = DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss");
                string fileModifyDate = fi.LastWriteTime.ToString("yyyy-MM-dd HH-mm-ss");

                lock (cacheLocker)
                {
                    InsertCache.Add(string.Format(@"('{0}','{1}','{2}','{3}','{4}','{5}')", now, filePathId, fileNameId, fileModifyDate, md5, histroyId));
                    if (InsertCache.Count >= CACHE_SIZE)
                    {
                        Save_cache();
                    }
                }
            }
            catch (Exception err)
            {
                throw err;
            }
        }

        public void Save_cache()
        {
            MySqlConnection mConn = new MySqlConnection(ConnectionString);
            List<string> temp = InsertCache;
            InsertCache = new List<string>();
            string errsql = "";
            try
            {
                if (temp.Count == 0) return;
                mConn.Open();

                MySqlCommand sqlcmd = mConn.CreateCommand();

                sqlcmd.CommandText = string.Format("INSERT INTO `{0}` (`backup_date`,`filepath_id`,`filename_id`,`modifydate`,`md5`,`histroy_id`)VALUES ",currentBackupTable);
                foreach (string values in temp)
                {
                    sqlcmd.CommandText += values + ",";
                }
                sqlcmd.CommandText = sqlcmd.CommandText.Substring(0, sqlcmd.CommandText.Length - 1);
                errsql = sqlcmd.CommandText;
                sqlcmd.ExecuteNonQuery();
            }
            catch (Exception err)
            {
                Console.WriteLine(errsql);
                throw err;
            }
            finally
            {
                mConn.Dispose();
            }
        }


        public string GetFolderPathId(string fpath, string parentFoloderId)
        {
            lock (folderLocker)
            {
                string retid = "";
                if (path_dict.ContainsKey(fpath))
                {
                    return path_dict[fpath];
                }
                else
                {
                    MySqlConnection mConn = new MySqlConnection(ConnectionString);
                    try
                    {
                        MySqlCommand sqlcmd = mConn.CreateCommand();

                        mConn.Open();
                        sqlcmd.CommandText = string.Format(@"INSERT INTO `tb_path` (`path`,`parentId`) VALUES ('{0}',{1});", fpath.Replace("\\", "\\\\").Replace("'", "\\'"), parentFoloderId);

                        sqlcmd.ExecuteNonQuery();
                        retid = sqlcmd.LastInsertedId.ToString();
                        path_dict.Add(fpath, retid);
                        return retid;

                    }
                    catch (Exception e)
                    {
                        throw e;
                    }
                    finally
                    {
                        mConn.Dispose();
                    }
                }
            }
        }

        public bool CheckExist(string md5)
        {
            return exist_md5.Contains(md5);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="datetime">备份日期</param>
        /// <returns>返回数据库中根目录id列表</returns>
        public Dictionary<int, ID_Table> GetRestoreBasePath(DateTime datetime)
        {
            Dictionary<int, ID_Table> basePathIds = new Dictionary<int, ID_Table>();            
            string selectCommand = string.Format(@"select id,path_id,backup_table from histroy where backup_date between '{0}' and '{1}'", 
                datetime.ToString("yyyy-MM-dd HH-mm-ss"), datetime.AddHours(23.9).ToString("yyyy-MM-dd HH-mm-ss"));
            Console.WriteLine(selectCommand);
            MySqlDataAdapter adp = new MySqlDataAdapter(selectCommand, conn);

            DataTable dt = new DataTable();
            adp.Fill(dt);
            //basePathIds = new int[dt.Rows.Count];

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                ID_Table table = new ID_Table();
                int path_id;
                int histroy_id;
                int.TryParse(dt.Rows[i]["path_id"].ToString(), out path_id);
                int.TryParse(dt.Rows[i]["id"].ToString(), out histroy_id);
                table.histroy_id = histroy_id;
                table.table_name = dt.Rows[i]["backup_table"].ToString();
                if (!basePathIds.ContainsKey(path_id))
                {
                    basePathIds.Add(path_id, table);
                }
                //basePathIds[i] = (int)dt.Rows[i]["id"];
            }
            //Console.WriteLine(basePathIds.Keys.Count);
            return basePathIds;
        }

        public DataTable GetChildDirectory(int id)
        {
            string selectCommand = string.Format(@"select id, path, parentid 
from tb_path 
where parentid = {0};", id);
            MySqlDataAdapter adp = new MySqlDataAdapter(selectCommand, conn);
            DataTable dt = new DataTable();
            adp.Fill(dt);
            return dt;
        }
        public DataTable GetPathList(Dictionary<int,ID_Table>.KeyCollection ids)
        {
            string selectCommand = @"select id, path, parentid from tb_path where id in (";
            foreach (int i in ids)
            {
                selectCommand += i.ToString() + ",";
            }

            selectCommand += "'');";
            Console.WriteLine(selectCommand);
            MySqlDataAdapter adp = new MySqlDataAdapter(selectCommand, conn);

            DataTable dt = new DataTable();
            adp.Fill(dt);

            return dt;
        }
    }
}
