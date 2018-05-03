using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.IO;
using System.Security.Cryptography;
using System.Diagnostics;
using System.Threading;
using SevenZip;


namespace backupCommand
{
    class BackupFolderParameter
    {
        public DirectoryInfo folder;
        public StreamWriter sw;
        public String parendFolderId;
        public string histroyId;
    }
    class Program
    {
        static public SqlClass sql;

        static public string PASSWORD = "avic64";

        public static int MINSIZE = 1024 * 16;
        public static int QUICK_HASH_SIZE = 1024 * 1024 * 100;
        public static int BUFSIZE = 1024 * 64;

        public static List<String> ignore_file = new List<string>();
        public static List<String> ignore_Folder = new List<string>();

        public static List<DirectoryInfo> backupFolderList = new List<DirectoryInfo>();
        public static List<DirectoryInfo> excludeFolderList = new List<DirectoryInfo>();
        private static string backupTargetDir;
        private static int mThreadCount = 0;

        public static readonly object countObj = new object();
        public static readonly object logWriter = new object();

        static void readDef(string defFile)
        {
            if (File.Exists(defFile))
            {
                StreamReader sr = new StreamReader(defFile, Encoding.UTF8);

                while (!sr.EndOfStream)
                {
                    string[] line = sr.ReadLine().Split('=');
                    if (line.Length > 1 && line[0].Trim().ToUpper() == "SAVE_FOLDER")
                    {
                        backupTargetDir = line[1];
                    }
                    else if (line.Length > 1 && line[0].Trim().ToUpper() == "IGNORE_FILE")
                    {
                        string[] temp = line[1].Split(',');
                        for (int i = 0; i < temp.Length; i++)
                        {
                            ignore_file.Add(string.Format("^{0}$", temp[i].Replace("$", "\\$").ToLower().Trim().Replace("*", ".*")));
                        }
                    }
                    else if (line.Length > 1 && line[0].Trim().ToUpper() == "IGNORE_FOLDER_KEY")
                    {
                        string[] temp = line[1].Split(',');
                        for (int i = 0; i < temp.Length; i++)
                        {
                            ignore_Folder.Add(temp[i].Replace("$", "\\$").ToLower().Trim());
                        }
                    }
                    else if (line.Length > 1 && line[0].Trim().ToUpper() == "BACKUP_FOLDER")
                    {
                        string[] temp = line[1].Split(',');

                        DirectoryInfo souceFolder = new DirectoryInfo(temp[0].Trim());
                        int intervalDay = 1;
                        int variableDay = 0;

                        try { intervalDay = Convert.ToInt32(temp[1]); } catch { }

                        try { variableDay = Convert.ToInt32(temp[2]); } catch { }

                        if (DateTime.Now.DayOfYear % intervalDay == variableDay)
                        {
                            backupFolderList.Add(souceFolder);
                        }
                    }
                    else if (line.Length > 1 && line[0].Trim().ToUpper() == "EXCLUDE_FOLDER")
                    {
                        DirectoryInfo excludeFolder = new DirectoryInfo(line[1].Trim());
                        excludeFolderList.Add(excludeFolder);
                    }
                    else if (line.Length > 1 && line[0].Trim().ToUpper() == "SERVERHOST")
                    {
                        SqlClass.SERVER_HOST = line[1].Trim();
                    }
                    else if (line.Length > 1 && line[0].Trim().ToUpper() == "DATABASE_NAME")
                    {
                        SqlClass.DATABASE_NAME = line[1].Trim();
                    }
                    else if (line.Length > 1 && line[0].Trim().ToUpper() == "USER")
                    {
                        SqlClass.SQL_USER = line[1].Trim();
                    }
                    else if (line.Length > 1 && line[0].Trim().ToUpper() == "PASSWORD")
                    {
                        SqlClass.SQL_PASSWORD = line[1].Trim();
                    }
                }
            }
        }
        static void Main(string[] args)
        {
            FileInfo curFile = new FileInfo(Process.GetCurrentProcess().MainModule.FileName);
            string defFile = string.Format("{0}\\backup.def", curFile.DirectoryName);

            readDef(defFile);

            sql = SqlClass.GetInstance();

            ThreadPool.SetMaxThreads(20, 20);
            string log_directory = string.Format("{0}\\log\\{1}", curFile.DirectoryName, DateTime.Now.ToString("yyyy-M-d"));
            if (!Directory.Exists(log_directory)) Directory.CreateDirectory(log_directory);

            StreamWriter log = new StreamWriter(string.Format("{0}\\{1}.txt", log_directory, DateTime.Now.ToString("yyyy-M-d HH-mm-ss")));

            try
            {
                sql.init();
                foreach (DirectoryInfo folder in backupFolderList)
                {
                    StreamWriter sw = new StreamWriter(string.Format("{0}\\{1}_{2}.txt", log_directory, folder.Name, DateTime.Now.ToString("yyyy-M-d HH-mm-ss")));
                    DateTime dt = DateTime.Now;
                    if (folder.Exists)
                    {
                        BackupFolderParameter p = new BackupFolderParameter();
                        p.folder = folder;
                        p.parendFolderId = "-1";
                        p.sw = sw;
                        p.histroyId = sql.getNewBackupHistroyId(sql.getFolderPathId(p.folder.FullName, p.parendFolderId));
                        //Thread thread = new Thread(new ParameterizedThreadStart(backupFolder));
                        lock (countObj) mThreadCount++;
                        //thread.Start(p);
                        //Console.WriteLine(string.Format("Thread:{0}",p.folder.FullName));
                        ThreadPool.QueueUserWorkItem(new WaitCallback(backupFolder), p);
                        //backupFolder(folder, "-1", sw);
                    }
                    else
                    {
                        lock (logWriter)
                        {
                            sw.WriteLine(string.Format("{0} 路径错误\n", folder.FullName));
                        }
                    }
                    //sw.WriteLine("Hash 文件夹 {0}", folder.FullName);
                    //DateTime dt1 = DateTime.Now;
                    //TimeSpan ts = dt1 - dt;
                    //sw.WriteLine("总用时:{0} 小时 {1} 分钟 {2} 秒", ts.Hours, ts.Minutes, ts.Seconds);
                    //sw.Close();
                }
                //sql.save_cache();
            }
            catch (Exception e)
            {
                log.WriteLine(e.Message);
                log.WriteLine(e.StackTrace);
            }
            //sql.save_cache();
            
            while (true)
            {
                Thread.Sleep(1000);
                Console.WriteLine("Thread Count:" + mThreadCount.ToString());
                if (mThreadCount <= 0)
                {
                    sql.save_cache();
                    break;
                }
            }
            Console.WriteLine("BackupFinished" + mThreadCount.ToString());
            log.Close();
        }

        public static bool isIgnore(FileInfo fileInfo)
        {
            foreach (string pattern in ignore_file)
            {
                if (Regex.IsMatch(fileInfo.Name.ToLower(), pattern))
                {
                    return true;
                }
            }
            return false;
        }

        public static bool isIgnoreFolder(DirectoryInfo dInfo)
        {
            foreach(string pattern in ignore_Folder)
            {
                if (Regex.IsMatch(dInfo.Name.ToLower(), pattern))
                {
                    return true;
                }
            }
            return false;
        }

        static private void backupFolder(object obj)//DirectoryInfo folder, string parentFolderId, StreamWriter sw)
        {
            DirectoryInfo folder = ((BackupFolderParameter)obj).folder;
            String parentFolderId = ((BackupFolderParameter)obj).parendFolderId;
            StreamWriter sw = ((BackupFolderParameter)obj).sw;
            string histroyId = ((BackupFolderParameter)obj).histroyId;
            try
            {
                foreach (DirectoryInfo excludeFolder in excludeFolderList)
                {
                    if (excludeFolder.FullName.Equals(folder.FullName))
                    {
                        return;
                    }
                }
                if (isIgnoreFolder(folder)) return;

                FileInfo[] files = folder.GetFiles();
                string folderId = sql.getFolderPathId(folder.FullName, parentFolderId);
                foreach (FileInfo fileInfo in files)
                {
                    try
                    {
                        if (fileInfo.Length > 0 && !isIgnore(fileInfo))
                        {
                            string md5 = quick_hash(fileInfo, sw);
                            if (md5.Length > 5)
                            {
                                if (!sql.checkExist(md5))
                                {
                                    //Console.WriteLine(String.Format("BackupFile:{0} {1}", md5, fileInfo.FullName));
                                    Backup(fileInfo, new DirectoryInfo(backupTargetDir), md5, sw);
                                    sql.AddMd5(md5);
                                }

                                sql.insert(fileInfo, md5, folderId,histroyId);
                            }
                        }
                    }
                    catch (Exception err)
                    {
                        Console.WriteLine(err.Message);
                        Console.WriteLine(err.StackTrace);
                        lock (logWriter)
                        {
                            sw.WriteLine(DateTime.Now.ToString());
                            sw.WriteLine("File:" + fileInfo.FullName);
                            sw.WriteLine(err.Message);
                            sw.WriteLine(err.StackTrace);
                            sw.Flush();
                        }
                    }
                }

                DirectoryInfo[] folders = folder.GetDirectories();

                foreach (DirectoryInfo subfolder in folders)
                {
                    //Thread thread = new Thread(new ParameterizedThreadStart(backupFolder));                    
                    BackupFolderParameter p = new BackupFolderParameter();
                    p.folder = subfolder;
                    p.parendFolderId = folderId;
                    
                    p.sw = sw;
                    p.histroyId = histroyId;
                    lock (countObj) mThreadCount++;
                    //thread.Start(p);
                    //Console.WriteLine(string.Format("Thread:{0}", p.folder.FullName));
                    ThreadPool.QueueUserWorkItem(new WaitCallback(backupFolder), p);
                    //backupFolder(p);
                }
            }
            catch(Exception err)
            {
                //Console.WriteLine(err.StackTrace);
                lock (logWriter)
                {
                    sw.WriteLine("Folder: " + folder.FullName);
                    sw.Write(err.Message);
                    sw.WriteLine(err.StackTrace);

                    sw.Flush();
                }
            }

            lock (countObj) mThreadCount--;
        }

        static void Backup(FileInfo sourceFile, DirectoryInfo targetFolder, string md5, StreamWriter log)
        {
            try
            {
                if (!targetFolder.Exists)
                {
                    targetFolder.Create();
                }

                string sub1 = md5.Substring(0, 2);
                string sub2 = md5.Substring(2, 2);
                string sub3 = md5.Substring(4, 2);

                DirectoryInfo secondDirctory = new DirectoryInfo(String.Format("{0}\\{1}\\{2}", targetFolder, sub1, sub2));                
                DirectoryInfo targetDirctory = new DirectoryInfo(String.Format("{0}\\{1}\\{2}\\{3}", targetFolder, sub1, sub2,sub3));
                if (!targetDirctory.Exists) targetDirctory.Create();

                MoveFileToThrid(string.Format("{0}\\{1}.7z", secondDirctory.FullName, md5));
                FileInfo targetFile = new FileInfo(string.Format("{0}\\{1}.7z", targetDirctory.FullName, md5));

                //压缩文件正没有正常结束时， 压缩包是坏的， 需要删除重新压缩
                try
                {
                    var extractor = new SevenZipExtractor(targetFile.FullName);
                    var t = extractor.IsSolid;
                }
                catch
                {
                    try
                    {
                        targetFile.Delete();
                    }
                    catch (Exception err)
                    {
                        log.WriteLine(err.Message);
                        log.WriteLine(err.StackTrace);
                    }
                }

                if (!targetFile.Exists)
                {
                    SevenZipCompressor tmp = new SevenZipCompressor();
                    tmp.CompressFilesEncrypted(targetFile.FullName, PASSWORD, new string[] { sourceFile.FullName });
                }
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
                throw new Exception("压缩文件错误"+sourceFile.FullName);
            }
        }

        private static void MoveFileToThrid(string filePath)
        {
            FileInfo f = new FileInfo(filePath);
            if (f.Exists)
            {
                DirectoryInfo thrid = new DirectoryInfo(f.Directory.FullName + "\\" + f.Name.Substring(4, 2));
                f.MoveTo(thrid.FullName + "\\" + f.Name);
            }
        }

        
        static string quick_hash(FileInfo fileInfo, StreamWriter sw)
        {
            string md5 = string.Empty;
            try
            {
                //FileInfo fileInfo = new FileInfo(FilePath);
                using (Stream inputStream = File.Open(fileInfo.FullName, FileMode.Open, FileAccess.Read, FileShare.Read))
                {
                    int MINSIZE = 1024 * 16;
                    int QUICK_HASH_SIZE = 1024 * 1024 * 100;
                    int BUFSIZE = 1024 * 64;

                    byte[] buffer = new byte[BUFSIZE];

                    long file_size = fileInfo.Length;

                    int block_count = 0;

                    if (file_size > QUICK_HASH_SIZE)
                    {
                        block_count = QUICK_HASH_SIZE / BUFSIZE;
                    }
                    else
                    {
                        block_count = (int)(file_size / BUFSIZE);
                    }

                    long offset = 0;
                    int readLength = 0;
                    long block_size = file_size;

                    if (block_count > 0)
                    {
                        block_size = file_size / block_count;
                    }

                    MD5CryptoServiceProvider hasher = new MD5CryptoServiceProvider();

                    int i = 0;
                    while (file_size - inputStream.Position > block_size)
                    {

                        offset = (i * block_size / MINSIZE) * MINSIZE;
                        inputStream.Seek(offset, SeekOrigin.Begin);

                        readLength = inputStream.Read(buffer, 0, (int)BUFSIZE);
                        hasher.TransformBlock(buffer, 0, readLength, buffer, 0);
                        i++;
                    }

                    byte[] lastBuffer = new byte[file_size - inputStream.Position];

                    readLength = inputStream.Read(lastBuffer, 0, (int)(file_size - inputStream.Position));

                    if (readLength > BUFSIZE)
                    {
                        hasher.TransformFinalBlock(lastBuffer, lastBuffer.Length - BUFSIZE, BUFSIZE);
                    }
                    else
                    {
                        hasher.TransformFinalBlock(lastBuffer, 0, readLength);
                    }
                    md5 = BitConverter.ToString(hasher.Hash);
                    hasher.Clear();
                    inputStream.Close();
                    md5 = md5.Replace("-", "");
                }
                //Console.WriteLine(md5);
            }
            catch (Exception e)
            {
                //Console.WriteLine(e.StackTrace);
                lock (logWriter)
                {
                    sw.WriteLine(string.Format("Hash文件出错， 文件名：{0}", fileInfo.FullName));
                    sw.WriteLine(e.Message);
                    sw.WriteLine(e.StackTrace);
                }
            }
            return md5;
        }
    }
}
