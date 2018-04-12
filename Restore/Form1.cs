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

namespace Restore
{
    public struct MyNode
    {
        public int id;
        public bool loaded;
        public int depth;
    }

    public partial class Form1 : Form
    {

        public SqlClass sql;
        public static string SAVE_FOLDER;        
        static public string PASSWORD = "avic64";
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
        private void dateTimePicker1_ValueChanged(object sender, EventArgs e)
        {
            treeView1.Nodes.Clear();
            DateTime dt;
            DateTime.TryParse(dateTimePicker1.Text, out dt);
            int[] basePathIds = sql.getRestoreBasePath(dt);
            textBox1.Text = basePathIds.Length.ToString();
            DataTable pathTable = sql.getPathList(basePathIds);
            foreach (DataRow row in pathTable.Rows)
            {
                DirectoryInfo dirInfo = new DirectoryInfo(row["path"].ToString());

                TreeNode node = new TreeNode(dirInfo.Name, 0, 0);
                MyNode nodeTag = new MyNode();
                nodeTag.id = (int)row["id"];
                nodeTag.loaded = false;

                node.Tag = nodeTag;
                node.Nodes.Add("None");
                treeView1.Nodes.Add(node);
            }
        }

        private void treeView1_BeforeExpand(object sender, TreeViewCancelEventArgs e)
        {
            TreeNode currNode= e.Node;
            MyNode tag = (MyNode)currNode.Tag;
            if (!tag.loaded)
            {
                currNode.Nodes.Clear();
                tag.loaded = true;

                currNode.Tag = tag;

                DataTable pathTable = sql.getChildDirectory(tag.id);


                foreach (DataRow row in pathTable.Rows)
                {
                    DirectoryInfo dirInfo = new DirectoryInfo(row["path"].ToString());

                    TreeNode node = new TreeNode(dirInfo.Name, 0, 0);
                    MyNode nodeTag = new MyNode();
                    nodeTag.id = (int)row["id"];
                    nodeTag.loaded = false;

                    node.Tag = nodeTag;
                    node.Nodes.Add("None");
                    currNode.Nodes.Add(node);
                }
            }
        }

        private void treeView1_AfterSelect(object sender, TreeViewEventArgs e)
        {
            listView1.Items.Clear();
            MyNode nodeTag = (MyNode)e.Node.Tag;
            DataTable dt = sql.getFileList(DateTime.Parse(dateTimePicker1.Text), nodeTag.id);
            Console.WriteLine(dt.Rows.Count);
            foreach(DataRow row in dt.Rows)
            {
                ListViewItem item = new ListViewItem(new string[] { row["filename"].ToString(),row["md5"].ToString()},1);
                listView1.Items.Add(item);
            }
        }

        private void btn_restore_file_Click(object sender, EventArgs e)
        {
            //string zipfile = @"h:\temp\test\27\B4\27B40DDC6FCB19D4D26940A0107B8001.7z";

            //var extractor = new SevenZipExtractor(zipfile, PASSWORD);
            //extractor.ExtractArchive("c:\\temp");

            FolderBrowserDialog sfd = new FolderBrowserDialog();
            if (sfd.ShowDialog() == DialogResult.OK)
            {
                foreach (ListViewItem item in listView1.SelectedItems)
                {
                    string md5 = item.SubItems[1].Text;
                    string zipFile = string.Format(@"{0}{1}\{2}\{3}.7z", SAVE_FOLDER, md5.Substring(0, 2), md5.Substring(2, 2), md5);
                    if (File.Exists(zipFile))
                    {
                        Console.WriteLine(string.Format("{0}   {1}    {2}",sfd.SelectedPath,zipFile,item.Text));
                        var extractor = new SevenZipExtractor(zipFile, PASSWORD);
                        extractor.ExtractArchive(sfd.SelectedPath);
                    }
                }
            }
        }
    }
}
