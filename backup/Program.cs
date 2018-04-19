using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.IO;
using System.Security.Cryptography;
using System.Diagnostics;
using SevenZip;


namespace backupCommand
{
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
            sql.init();


            string log_directory = string.Format("{0}\\log\\{1}", curFile.DirectoryName, DateTime.Now.ToString("yyyy-M-d"));
            if (!Directory.Exists(log_directory)) Directory.CreateDirectory(log_directory);

            foreach (DirectoryInfo folder in backupFolderList)
            {
                StreamWriter sw = new StreamWriter(string.Format("{0}\\{1}_{2}.txt", log_directory, folder.Name, DateTime.Now.ToString("yyyy-M-d HH-mm-ss")));
                DateTime dt = DateTime.Now;
                if (folder.Exists)
                {
                    backupFolder(folder, "-1", sw);
                }
                else
                {
                    sw.WriteLine(string.Format("{0} 路径错误\n", folder.FullName));
                }
                sw.WriteLine("Hash 文件夹 {0}", folder.FullName);
                DateTime dt1 = DateTime.Now;
                TimeSpan ts = dt1 - dt;
                sw.WriteLine("总用时:{0} 小时 {1} 分钟 {2} 秒", ts.Hours, ts.Minutes, ts.Seconds);
                sw.Close();
            }
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

        static private void backupFolder(DirectoryInfo folder, string parentFloderId, StreamWriter sw)
        {
            foreach(DirectoryInfo excludeFloder in excludeFolderList)
            {
                if(excludeFloder.FullName.Equals(folder.FullName))
                {
                    return;
                }
            }
            if (isIgnoreFolder(folder)) return;

            FileInfo[] files = folder.GetFiles();
            string floderId = sql.getFolderPathId(folder.FullName, parentFloderId);
            foreach (FileInfo fileInfo in files)
            {
                try
                {
                    if (fileInfo.Length > 0 && !isIgnore(fileInfo))
                    {
                        string md5 = quick_hash(fileInfo, sw);
                        if (md5.Length > 5)
                        {
                            Backup(fileInfo, new DirectoryInfo(backupTargetDir), md5, sw);

                            sql.insert(fileInfo, md5, floderId);
                        }
                    }
                }
                catch (Exception err)
                {
                    sw.WriteLine(DateTime.Now.ToString());
                    sw.WriteLine("File:" + fileInfo.FullName);
                    sw.WriteLine(err.Message);
                    sw.WriteLine(err.StackTrace);
                }
            }

            DirectoryInfo[] folders = folder.GetDirectories();

            foreach (DirectoryInfo subfolder in folders)
            {
                backupFolder(subfolder, floderId, sw);
            }
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

                MoveFileToThrid(string.Format("{0}\\{1}.7z", secondDirctory.FullName, md5));

                if (!targetDirctory.Exists) targetDirctory.Create();

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
            catch (Exception e)
            {
                log.WriteLine(e.Message);
                log.WriteLine(e.StackTrace);
            }
        }

        private static void MoveFileToThrid(string filePath)
        {
            FileInfo f = new FileInfo(filePath);
            if (f.Exists)
            {
                DirectoryInfo thrid = new DirectoryInfo(f.Directory.FullName + "\\" + f.Name.Substring(4, 2));
                if (!thrid.Exists) thrid.Create();

                f.MoveTo(thrid.FullName + "\\" + f.Name);
            }
        }

        private static string GetMD5Hash(string FilePath)
        {
            FileStream fs = null;
            string tempPath = System.Environment.GetEnvironmentVariable("TMP");

            string hash = "";
            byte[] hashBytes;
            MD5CryptoServiceProvider hasher = new MD5CryptoServiceProvider();

            try
            {
                fs = new FileStream(FilePath, FileMode.Open, FileAccess.Read, FileShare.Read);
                hashBytes = hasher.ComputeHash(fs);
                hash = BitConverter.ToString(hashBytes);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.ToString());
            }
            finally { if (fs != null) fs.Close(); }

            return hash.Replace("-", "");
        }

        private static string get_filemd5(string FilePath)
        {
            try
            {
                FileInfo fileInfo = new FileInfo(FilePath);

                if (fileInfo.Exists)
                {
                    long file_size = fileInfo.Length;

                    /*
                    其中MINSIZE定义了对齐的大小，BUFSIZE定义了HASH分块的大小，QUICK_HASH_SIZE则是定义了需要做快速HASH的文件大小阈值。
                    方法很简单：文件小于QUICK_HASH_SIZE，则做全文件HASH，大于QUICK_HASH_SIZE则按BUFSIZE大小间隔取样，总取样数据大小为QUICK_HASH_SIZE。
                    每个BUFSIZE块在文件中的分布为均匀的，但是每块又以MINSIZE对齐，另外，因为通常文件最经常被修改的部分是最后的部分，
                    所以最后一块一定是与文件结尾对齐，以保证HASH部分包括文件的结尾。
                    block_count = int((QUICK_HASH_SIZE + BUFSIZE / 2) / BUFSIZE
                              if QUICK_HASH_SIZE else file_size / BUFSIZE)
                    block_size = file_size * 1.0 / block_count if QUICK_HASH_SIZE \
                        else BUFSIZ
                    */
                    long block_count;

                    if (file_size > QUICK_HASH_SIZE)
                    {
                        block_count = (QUICK_HASH_SIZE + BUFSIZE / 2) / BUFSIZE;
                    }
                    else
                    {
                        block_count = file_size / BUFSIZE + 1;
                    }

                    long block_size = file_size / block_count;

                    int bufferSize = int.Parse(BUFSIZE.ToString());//自定义缓冲区大小16K  

                    byte[] buffer = new byte[bufferSize];


                    HashAlgorithm hashAlgorithm = new MD5CryptoServiceProvider();


                    MD5CryptoServiceProvider md5Provider = new MD5CryptoServiceProvider();

                    Stream inputStream = File.Open(FilePath, FileMode.Open, FileAccess.Read, FileShare.Read);

                    int readLength = 0;//每次读取长度  
                    var output = new byte[bufferSize];
                    //inputStream.Read();
                    for (int i = 0; i < block_count; i++)
                    {
                        long pos = (long)(i * block_size / MINSIZE) * MINSIZE;
                        inputStream.Seek(pos, SeekOrigin.Begin);
                        readLength = inputStream.Read(buffer, 0, (int)BUFSIZE);

                        Console.WriteLine(String.Format("i={0}:{1}", i, Encoding.UTF8.GetString(buffer)));
                        //Console.WriteLine(pos);
                        //计算MD5  
                        hashAlgorithm.TransformBlock(buffer, 0, readLength, output, 0);
                    }
                    //完成最后计算，必须调用(由于上一部循环已经完成所有运算，所以调用此方法时后面的两个参数都为0)  
                    hashAlgorithm.TransformFinalBlock(buffer, 0, 0);
                    string md5 = BitConverter.ToString(hashAlgorithm.Hash);
                    hashAlgorithm.Clear();
                    inputStream.Close();
                    md5 = md5.Replace("-", "");
                    return md5;

                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
            return string.Empty;
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
                sw.WriteLine(string.Format("Hash文件出错， 文件名：{0}", fileInfo.FullName));
                sw.WriteLine(e.Message);
                sw.WriteLine(e.StackTrace);
            }
            return md5;
        }
    }
}
