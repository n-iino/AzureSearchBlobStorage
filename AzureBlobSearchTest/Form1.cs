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
            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve a reference to a container.
            CloudBlobContainer container = blobClient.GetContainerReference("testcontainer");

            //スリープしないと初回押下時にCreateIfNotExistsでエラー吐かれたりする　処理中にアクセスしてる？
            //ちゃんと書くなら例外処理を真面目に書く必要あり
            Thread.Sleep(2000);

            // Create the container if it doesn't already exist.
            container.CreateIfNotExists();

            // Retrieve storage account from connection string.
            storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the blob client.
            blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            container = blobClient.GetContainerReference("testcontainer");

            // Retrieve reference to a blob named "myblob".
            CloudBlockBlob blockBlob = container.GetBlockBlobReference("test.csv");
            
            //アップロード用の適当なファイル作る
            if (!System.IO.File.Exists("test.csv"))
            {
                using (System.IO.FileStream hStream = System.IO.File.Create("test.csv"))
                {
                    if (hStream != null)
                    {
                        hStream.Close();
                    }
                }
                
            }

            using (System.IO.StreamWriter sw = new System.IO.StreamWriter("test.csv"))
                sw.Write("Id,Name,Age\n0,iino,21\n1,suzuki,25\n2,山田,33");

            // Create or overwrite the "myblob" blob with contents from a local file.
            using (var fileStream = System.IO.File.OpenRead("test.csv"))
            {
                blockBlob.UploadFromStream(fileStream);
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
            // Retrieve storage account from connection string.
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(
                CloudConfigurationManager.GetSetting("StorageConnectionString"));

            // Create the blob client.
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // Retrieve reference to a previously created container.
            CloudBlobContainer container = blobClient.GetContainerReference("testcontainer");

            listBox1.Items.Clear();

            // Loop over items within the container and output the length and URI.
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


        /// <summary>
        /// インデックス作成
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private async void button3_Click(object sender, EventArgs e)
        {
            flg = false;

            var connection = ApiConnection.Create("datest01", "A53D975119F80AA1D75895645A5234BE");

            var client = new IndexManagementClient(connection);

            //前にあったデータは削除
            client.DeleteIndexAsync("index01").Wait();
            connection.Execute(new ApiRequest("datasources/testcontainer", HttpMethod.Delete), CancellationToken.None).Wait();
            connection.Execute(new ApiRequest("indexers/indexer01", HttpMethod.Delete), CancellationToken.None).Wait();

            //データソース設定
            connection.Execute(new ApiRequest("datasources", HttpMethod.Post, new 
            {
               
                    name = "source01",
                    type = "azureblob",
                    credentials = new { connectionString = "DefaultEndpointsProtocol=https;AccountName=testda01;AccountKey=Yh3sa6UyVcg8puiiBu1EYlesMSJLX45ZFwv0Ws8fxDjCin+5INxmMOBjwN0EaAN0TvFy92gjdfHHmas7lnpEzw==" },
                    container = new { name = "testcontainer" }

                
            }), CancellationToken.None).Wait();

            //インデックス作成
             connection.Execute(new ApiRequest("indexes", HttpMethod.Post, new
               {

                   name = "index01",
                   fields = new List<F>
                        {
                           new F
                           {
                               name = "Id",
                               type= "Edm.String",
                               key = true
                           },
                            new F
                           {
                               name = "Name",
                               type= "Edm.String",
                           },
                             new F
                           {
                               name = "Age",
                               type= "Edm.String",
                           }
                       }

               }), CancellationToken.None).Wait();

            //インデクサ設定(ヘッダーのみ)
            connection.Execute(new ApiRequest("indexers", HttpMethod.Post, new
            {

                name = "indexer01",
                dataSourceName = "source01",
                targetIndexName = "index01",
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
               
                    name = "indexer01",
                    dataSourceName = "source01",
                    targetIndexName = "index01",
                    parameters = new
                    {
                        configuration = new
                        {
                            parsingMode = "delimitedText",
                            delimitedTextHeaders = "Id,Name,Age",
                        }
                    }

                
            }), CancellationToken.None).Wait();


            connection.Execute(new ApiRequest("indexers/indexer01/run", HttpMethod.Post), CancellationToken.None).Wait();

            await Task.Run(()=> Thread.Sleep(5000));


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

            // SearchQueryの引数が検索したい文字列
            var result = await c.SearchAsync("index01", new SearchQuery(textBox1.Text).Count(true));

            Console.WriteLine("count = " + result.Body.Records.Count());


            textBox2.Clear();

            foreach (var item in result.Body.Records)
            {
                textBox2.AppendText(string.Format("Id={0} Name={1} Age={2}\n", item.Properties["Id"], item.Properties["Name"], item.Properties["Age"]));
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
    }
   
}
