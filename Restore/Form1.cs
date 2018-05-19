using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using backupCommand;
using System.IO;
using System.Diagnostics;
using SevenZip;
using System.Text.RegularExpressions;

namespace Restore
{
    public struct MyNodeTag
    {
        public int pathid;
        public bool loaded;
        public int histroy_id;
        public string backupTable;
    }

    public partial class Form1 : Form
    {

        public SqlClass sql;
        public static string SAVE_FOLDER;        
        static public string PASSWORD = "avic64";
        public static List<String> ignore_Folder = new List<string>();
        public static List<DirectoryInfo> excludeFolderList = new List<DirectoryInfo>();
        public Form1()
        {
            InitializeComponent();
        }

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
                        SAVE_FOLDER = line[1].Trim();
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
                    else if (line.Length > 1 && line[0].Trim().ToUpper() == "EXCLUDE_FOLDER")
                    {
                        DirectoryInfo excludeFolder = new DirectoryInfo(line[1].Trim());
                        excludeFolderList.Add(excludeFolder);
                    }
                }
            }
        }
        private void Form1_Load(object sender, EventArgs e)
        {
            FileInfo curFile = new FileInfo(Process.GetCurrentProcess().MainModule.FileName);
            string defFile = string.Format("{0}\\backup.def", curFile.DirectoryName);

            readDef(defFile);

            sql = SqlClass.GetInstance();

        }

        public static bool IsIgnoreFolder(DirectoryInfo dInfo)
        {
            foreach (string pattern in ignore_Folder)
            {
                if (Regex.IsMatch(dInfo.Name.ToLower(), pattern))
                {
                    return true;
                }
            }
            return false;
        }
        private void dateTimePicker1_ValueChanged(object sender, EventArgs e)
        {
            treeView1.Nodes.Clear();
            DateTime dt;
            DateTime.TryParse(dateTimePicker1.Text, out dt);

            Dictionary<int,ID_Table> basePathIds = sql.GetRestoreBasePath(dt);
            textBox1.Text = basePathIds.Keys.Count.ToString();
            

            DataTable pathTable = sql.GetPathList(basePathIds.Keys);
            foreach (DataRow row in pathTable.Rows)
            {
                DirectoryInfo dirInfo = new DirectoryInfo(row["path"].ToString());

                TreeNode node = new TreeNode(dirInfo.Name, 0, 0);
                MyNodeTag nodeTag = new MyNodeTag
                {
                    pathid = (int)row["id"],
                    loaded = false,
                    histroy_id = basePathIds[(int)row["id"]].histroy_id,
                    backupTable = basePathIds[(int)row["id"]].table_name                    
                };
                node.Tag = nodeTag;
                node.Nodes.Add("None");
                treeView1.Nodes.Add(node);
            }
        }

        private void treeView1_BeforeExpand(object sender, TreeViewCancelEventArgs e)
        {
            TreeNode currNode= e.Node;
            MyNodeTag tag = (MyNodeTag)currNode.Tag;
            if (!tag.loaded)
            {
                currNode.Nodes.Clear();
                tag.loaded = true;

                currNode.Tag = tag;

                DataTable pathTable = sql.GetChildDirectory(tag.pathid);


                foreach (DataRow row in pathTable.Rows)
                {
                    DirectoryInfo dirInfo = new DirectoryInfo(row["path"].ToString());

                    TreeNode node = new TreeNode(dirInfo.Name, 0, 0);
                    MyNodeTag nodeTag = new MyNodeTag
                    {
                        pathid = (int)row["id"],
                        loaded = false,
                        histroy_id = tag.histroy_id,
                        backupTable = tag.backupTable
                    };

                    node.Tag = nodeTag;
                    node.Nodes.Add("None");
                    currNode.Nodes.Add(node);
                }
            }
        }

        private void treeView1_AfterSelect(object sender, TreeViewEventArgs e)
        {
            listView1.Items.Clear();
            MyNodeTag nodeTag = (MyNodeTag)e.Node.Tag;
            textBox1.Text = nodeTag.pathid.ToString();
            DataTable dt = sql.GetFileList(nodeTag.histroy_id, nodeTag.pathid, nodeTag.backupTable);
            
            foreach(DataRow row in dt.Rows)
            {
                string md5 = row["md5"].ToString();

                string sub1 = md5.Substring(0, 2);
                string sub2 = md5.Substring(2, 2);
                string sub3 = md5.Substring(4, 2);

                DirectoryInfo d = new DirectoryInfo(SAVE_FOLDER);

                FileInfo zipFileInfo = new FileInfo(string.Format(@"{0}\{1}\{2}\{3}.7z", d.FullName, sub1, sub2, md5));
                if(zipFileInfo.Exists)
                {
                    d = new DirectoryInfo(d.FullName + "\\" + sub3);
                    if (!d.Exists) d.Create();
                    zipFileInfo.MoveTo(string.Format(@"{0}\{1}.7z", d.FullName, md5));
                }

                ListViewItem item = new ListViewItem(new string[] { row["filename"].ToString(),row["modifydate"].ToString(), row["md5"].ToString()},1);
                listView1.Items.Add(item);
            }
        }

        private void btn_restore_file_Click(object sender, EventArgs e)
        {
            FolderBrowserDialog sfd = new FolderBrowserDialog();
            if (sfd.ShowDialog() == DialogResult.OK)
            {
                foreach (ListViewItem item in listView1.SelectedItems)
                {
                    string md5 = item.SubItems[2].Text;

                    string sub1 = md5.Substring(0, 2);
                    string sub2 = md5.Substring(2, 2);
                    string sub3 = md5.Substring(4, 2);
                    DirectoryInfo d = new DirectoryInfo(SAVE_FOLDER);
                    string zipFile = string.Format(@"{0}\{1}\{2}\{3}\{4}.7z", d.FullName, sub1, sub2,sub3, md5);
                    if (File.Exists(zipFile))
                    {
                        var extractor = new SevenZipExtractor(zipFile, PASSWORD);
                        extractor.ExtractArchive(sfd.SelectedPath);
                    }
                    else
                    {
                        MessageBox.Show("备份文件不存在：" + zipFile);
                    }
                }
                if (MessageBox.Show("解压完毕,是否打开解压目录？", "", MessageBoxButtons.OKCancel, MessageBoxIcon.Information) == DialogResult.OK)
                {
                    Process.Start(sfd.SelectedPath);
                }
            }
        }

        private void btn_restore_directory_Click(object sender, EventArgs e)
        {

        }
    }
}
