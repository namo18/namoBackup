using System;
using System.Collections.Generic;
using System.Text;
using MySql.Data.MySqlClient;
using System.IO;
using System.Data;

namespace backupCommand
{
    public class SqlClass
    {
        private MySqlConnection conn;
        private static SqlClass instance;
        private static Dictionary<string, string> file_name_dict = new Dictionary<string, string>();
        private static Dictionary<string, string> path_dict = new Dictionary<string, string>();
        private static List<string> exist_md5 = new List<string>();
        public static string SERVER_HOST;
        public static string DATABASE_NAME;
        public static string SQL_USER;
        public static string SQL_PASSWORD;
        private List<string> InsertCache = new List<string>();

        private SqlClass()
        {
            string ConnectionString = string.Format(@"server={0};port=3306;user id={1};password={2};database={3};allow zero datetime=true", SERVER_HOST, SQL_USER, SQL_PASSWORD, DATABASE_NAME);
            this.conn = new MySqlConnection(ConnectionString);
        }

        public void init()
        {
            MySqlDataAdapter adp = new MySqlDataAdapter("select id,path from tb_path", conn);
            DataTable dt = new DataTable();
            adp.Fill(dt);
            foreach (DataRow row in dt.Rows)
            {
                path_dict.Add(row["path"].ToString(), row["id"].ToString());
            }
            dt.Clear();
            adp.SelectCommand.CommandText = "select id,filename from filename";
            adp.Fill(dt);
            foreach (DataRow row in dt.Rows)
            {
                file_name_dict.Add(row["filename"].ToString(), row["id"].ToString());
            }

            dt.Clear();
            adp.SelectCommand.CommandText = "select md5 from backupinfo group by md5";
            adp.Fill(dt);

            foreach (DataRow row in dt.Rows)
            {
                exist_md5.Add(row["md5"].ToString());
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

        public string getFileNameId(string filename)
        {
            if (file_name_dict.ContainsKey(filename))
            {
                return file_name_dict[filename];
            }
            else
            {
                try
                {
                    if (conn.State != ConnectionState.Open)
                    {
                        conn.Open();
                    }
                    MySqlCommand sqlcmd = conn.CreateCommand();
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
                    if (conn.State == ConnectionState.Open)
                    {
                        conn.Close();
                    }
                }
            }
        }


        private void Conn_StateChange(object sender, StateChangeEventArgs e)
        {
            throw new NotImplementedException();
        }

        public DataTable getFileList(DateTime datetime, int pathId)
        {
            DataTable table = new DataTable();
            try
            {
                string selectCommand = string.Format(@"select t1.id as id, t1.filename as filename, t2.md5 as md5 from filename as t1 right join 
(select filename_id,md5 from backupinfo where backup_date between '{0}' and '{1}' and filepath_id = {2}) as t2
on t2.filename_id = t1.id", datetime.ToString("yyyy-MM-dd"), datetime.AddDays(1).ToString("yyyy-MM-dd"), pathId);
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

        public void insert(FileInfo fi, string md5, string filePathId)
        {
            try
            {
                string fileNameId = getFileNameId(fi.Name);

                string now = DateTime.Now.ToString("yyyy-MM-dd HH-mm-ss");
                string fileModifyDate = fi.LastWriteTime.ToString("yyyy-MM-dd HH-mm-ss");

                InsertCache.Add(string.Format(@"('{0}','{1}','{2}','{3}','{4}')", now, filePathId, fileNameId, fileModifyDate, md5));

                if (InsertCache.Count > 1000)
                {
                    if (conn.State != ConnectionState.Open) conn.Open();

                    MySqlCommand sqlcmd = conn.CreateCommand();

                    sqlcmd.CommandText = "INSERT INTO `backupinfo` (`backup_date`,`filepath_id`,`filename_id`,`modifydate`,`md5`)VALUES ";
                    foreach (string values in InsertCache)
                    {
                        sqlcmd.CommandText += values + ",";
                    }
                    sqlcmd.CommandText = sqlcmd.CommandText.Substring(0, sqlcmd.CommandText.Length - 1);
                    sqlcmd.ExecuteNonQuery();

                    InsertCache.Clear();
                }
            }
            catch (Exception err)
            {
                throw err;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                {
                    conn.Close();
                }
            }
        }

        public string getFolderPathId(string fpath, string parentFoloderId)
        {
            string retid = "";
            if (path_dict.ContainsKey(fpath))
            {
                return path_dict[fpath];
            }
            else
            {
                try
                {
                    MySqlCommand sqlcmd = conn.CreateCommand();
                    conn.Open();
                    sqlcmd.CommandText = string.Format(@"INSERT INTO `tb_path` (`path`,`parentId`) VALUES ('{0}',{1});", fpath.Replace("\\", "\\\\").Replace("'", "\\'"), parentFoloderId);

                    sqlcmd.ExecuteNonQuery();
                    retid = sqlcmd.LastInsertedId.ToString();
                    conn.Close();
                    path_dict.Add(fpath, retid);
                    return retid;
                }
                catch (Exception e)
                {
                    throw e;
                }
                finally
                {
                    if (conn.State == ConnectionState.Open)
                    {
                        conn.Close();
                    }
                }
            }
        }

        public bool checkExist(string md5)
        {
            return exist_md5.Contains(md5);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="datetime">备份日期</param>
        /// <returns>返回数据库中根目录id列表</returns>
        public int[] getRestoreBasePath(DateTime datetime)
        {
            int[] basePathIds;
            string selectCommand = string.Format(@"select * from tb_path where id in (
SELECT filepath_id FROM filebackupsys.backupinfo
where backup_date between '{0}' and '{1}'
group by filepath_id
)
and depth = 1", datetime.ToString("yyyy-MM-dd"), datetime.AddDays(1).ToString("yyyy-MM-dd"));

            MySqlDataAdapter adp = new MySqlDataAdapter(selectCommand, conn);

            DataTable dt = new DataTable();
            adp.Fill(dt);
            basePathIds = new int[dt.Rows.Count];

            for (int i = 0; i < dt.Rows.Count; i++)
            {
                basePathIds[i] = (int)dt.Rows[i]["id"];
            }
            return basePathIds;
        }

        public DataTable getChildDirectory(int id)
        {
            string selectCommand = string.Format(@"select id, path, depth 
from tb_path 
where path like replace(concat(getPathByid({0}),'\\%'),'\\','\\\\') and depth = getpathDepthById({0})+1;", id);
            MySqlDataAdapter adp = new MySqlDataAdapter(selectCommand, conn);
            DataTable dt = new DataTable();
            adp.Fill(dt);
            return dt;
        }
        public DataTable getPathList(int[] ids)
        {
            string selectCommand = @"select id, path, depth from tb_path where id in (";
            foreach (int i in ids)
            {
                selectCommand += i.ToString() + ",";
            }

            selectCommand += "'');";

            MySqlDataAdapter adp = new MySqlDataAdapter(selectCommand, conn);

            DataTable dt = new DataTable();
            adp.Fill(dt);

            return dt;
        }
    }
}
