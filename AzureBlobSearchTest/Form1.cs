using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using Microsoft.Azure; // Namespace for CloudConfigurationManager
using Microsoft.WindowsAzure.Storage; // Namespace for CloudStorageAccount
using Microsoft.WindowsAzure.Storage.Blob; // Namespace for Blob storage types
using System.Threading;
using Microsoft.Azure.Search;
using System.Net;
using Newtonsoft.Json.Linq;
using System.Runtime.Serialization.Json;
using System.IO;
using RedDog.Search.Http;
using System.Net.Http;
using RedDog.Search;
using RedDog.Search.Model;

namespace AzureBlobSearchTest
{
    public partial class Form1 : Form
    {
        private bool flg = false;
        /// <summary>
        /// アップロードするファイルリスト
        /// </summary>
        private string[] fileNames = new string[] { "files\\csv\\test.csv", "files\\pdf\\test.pdf" , "files\\xls\\test.xlsx"};

        public Form1()
        {
            InitializeComponent();
        }
        
        /// <summary>
        /// blob storageにアップロード
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button1_Click(object sender, EventArgs e)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));
            
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            List<CloudBlobContainer> containers = new List<CloudBlobContainer>();

            for (int i = 0; i < fileNames.Length; i++)
            {
                var c = blobClient.GetContainerReference("testcontainer" + i);

                //スリープしないと初回押下時にCreateIfNotExistsでエラー吐かれたりする　処理中にアクセスしてる？
                //ちゃんと書くなら例外処理を真面目に書く必要あり
                Thread.Sleep(2000);
                c.CreateIfNotExists();
                containers.Add(c);
            }
            
            
            for (int i = 0; i < fileNames.Length; i++)
            {
                var c = blobClient.GetContainerReference("testcontainer" + i);
                var blockBlob = c.GetBlockBlobReference(fileNames[i]);

                //アップするファイル全取得
                using (var fileStream = File.OpenRead(fileNames[i]))
                {
                    //アップロード
                    blockBlob.UploadFromStream(fileStream);
                }
            }

            
            MessageBox.Show("ファイルをアップロードしました。");
        }

        /// <summary>
        /// blob storageから取得
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void button2_Click(object sender, EventArgs e)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));
            
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            listBox1.Items.Clear();

            for (int i = 0; i < fileNames.Length; i++)
            {
                CloudBlobContainer container = blobClient.GetContainerReference("testcontainer" + i);
                
                foreach (IListBlobItem item in container.ListBlobs(null, true))
                {

                    if (item.GetType() == typeof(CloudBlockBlob))
                    {
                        CloudBlockBlob blob = (CloudBlockBlob)item;

                        listBox1.Items.Add(string.Format("Block blob of length {0}: {1}", blob.Properties.Length, blob.Uri));

                    }
                    else if (item.GetType() == typeof(CloudPageBlob))
                    {
                        CloudPageBlob pageBlob = (CloudPageBlob)item;

                        listBox1.Items.Add(string.Format("Page blob of length {0}: {1}", pageBlob.Properties.Length, pageBlob.Uri));

                    }
                    else if (item.GetType() == typeof(CloudBlobDirectory))
                    {
                        CloudBlobDirectory directory = (CloudBlobDirectory)item;

                        listBox1.Items.Add(string.Format("Directory: {0}", directory.Uri));
                    }
                }
                
            }
        }


        /// <summary>
        /// インデックス作成
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private async void button3_Click(object sender, EventArgs e)
        {
            flg = false;


            //前にあったデータは削除
            for (int i = 0; i < fileNames.Length; i++)
            {
                var connection = ApiConnection.Create("datest01", "A53D975119F80AA1D75895645A5234BE");
                var client = new IndexManagementClient(connection);

                client.DeleteIndexAsync("index" + i).Wait();
                connection.Execute(new ApiRequest("datasources/testcontainer" + i, HttpMethod.Delete), CancellationToken.None).Wait();
                connection.Execute(new ApiRequest("indexers/indexer" + i, HttpMethod.Delete), CancellationToken.None).Wait();

                //データソース設定
                connection.Execute(new ApiRequest("datasources", HttpMethod.Post, new
                {
                    name = "source" + i,
                    type = "azureblob",
                    credentials = new { connectionString = "DefaultEndpointsProtocol=https;AccountName=testda01;AccountKey=Yh3sa6UyVcg8puiiBu1EYlesMSJLX45ZFwv0Ws8fxDjCin+5INxmMOBjwN0EaAN0TvFy92gjdfHHmas7lnpEzw==" },
                    container = new { name = "testcontainer" + i }


                }), CancellationToken.None).Wait();

                if (fileNames[i].Contains(".csv"))
                {
                    //インデックス作成
                    connection.Execute(new ApiRequest("indexes", HttpMethod.Post, new
                    {
                        name = "index" + i,
                        fields = new List<F>
                        {
                           new F
                           {
                               name = "metadata_storage_name",
                               type= "Edm.String",
                               searchable = true,
                               sortable = true,
                               facetable = true,
                               filterable = true
                           },

                           new F
                           {
                               name = "Id",
                               type= "Edm.String",
                               searchable = true,
                               key = true

                           },
                            new F
                           {
                               name = "Name",
                               type= "Edm.String",
                               searchable = true,
                           },
                             new F
                           {
                               name = "Age",
                               type= "Edm.String",
                               searchable = true,
                           }
                       }

                    }), CancellationToken.None).Wait();

                    //インデクサ設定(ヘッダーのみ)
                    connection.Execute(new ApiRequest("indexers", HttpMethod.Post, new
                    {

                        name = "indexer" + i,
                        dataSourceName = "source" + i,
                        targetIndexName = "index" + i,
                        parameters = new
                        {
                            configuration = new
                            {
                                parsingMode = "delimitedText",
                                firstLineContainsHeaders = true
                            }
                        }


                    }), CancellationToken.None).Wait();

                    //インデクサ作成(ヘッダー以外のみ)
                    connection.Execute(new ApiRequest("indexers", HttpMethod.Post, new
                    {

                        name = "indexer" + i,
                        dataSourceName = "source" + i,
                        targetIndexName = "index" + i,
                        parameters = new
                        {
                            configuration = new
                            {
                                parsingMode = "delimitedText",
                                delimitedTextHeaders = "Id,Name,Age",
                            }
                        }


                    }), CancellationToken.None).Wait();


                    connection.Execute(new ApiRequest("indexers/indexer" + i + "/run", HttpMethod.Post), CancellationToken.None).Wait();

                }
               
                else
                {

                    //インデックス作成
                    connection.Execute(new ApiRequest("indexes", HttpMethod.Post, new
                    {
                        name = "index" + i,
                        fields = new List<F>
                        {

                             new F
                           {
                               name = "metadata_storage_content_md5",
                               type= "Edm.String",
                               searchable = true,
                               sortable = true,
                               facetable = true,
                               filterable = true,
                               key = true
                           },

                           new F
                           {
                               name = "metadata_storage_name",
                               type= "Edm.String",
                               searchable = true,
                               sortable = true,
                               facetable = true,
                               filterable = true,
                           },
                            new F
                           {
                               name = "content",
                               type= "Edm.String",
                               searchable = true,
                               sortable = true,
                               facetable = true,
                               filterable = true
                           }
                       }

                    }), CancellationToken.None).Wait();

                    //インデクサ設定
                    connection.Execute(new ApiRequest("indexers", HttpMethod.Post, new
                    {

                        name = "indexer" + i,
                        dataSourceName = "source" + i,
                        targetIndexName = "index" + i,


                    }), CancellationToken.None).Wait();

                    connection.Execute(new ApiRequest("indexers/indexer" + i + "/run", HttpMethod.Post), CancellationToken.None).Wait();

                }
                


            }


            await Task.Run(()=> Thread.Sleep(5000));
            MessageBox.Show("インデックスの設定が終了したよ");

            flg = true;
        }
        
        /// <summary>
        /// 検索
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private async void button4_Click(object sender, EventArgs e)
        {
            //インデクサ実行終了までにはAPIのレスポンス帰ってきてから少し時間がかかる模様　とりあえずwaitする
            if (!flg)
            {
                MessageBox.Show("インデックスを作成してね。作成したら少し時間をおいて(5秒くらい)アクセスしてね");
                return;
            }
            var connection = ApiConnection.Create("datest01", "A53D975119F80AA1D75895645A5234BE");

            // 検索を行う場合は IndexQueryClient を使う
            var c = new IndexQueryClient(connection);

            textBox2.Clear();

            for (int i = 0; i < fileNames.Length; i++)
            {
                // SearchQueryの引数が検索したい文字列
                var result = await c.SearchAsync("index" + i, new SearchQuery(textBox1.Text).Count(true));

                if (result.StatusCode == HttpStatusCode.NotFound) continue;

               

                foreach (var item in result.Body.Records)
                {
                   textBox2.AppendText(string.Format("filename={0}\n", item.Properties["metadata_storage_name"]));
                }
            }
            
        }
    }

    /// <summary>
    ///　ＡＰＩリクエスト用
    /// </summary>
    class F
    {
        public string name { get; set; }
        public string type { get; set; }
        public bool key { get; set; }
        public bool searchable { get; set; }
        public bool filterable { get; set; }
        public bool sortable { get; set; }
        public bool facetable { get; set; }
    }
   
}
