using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Windows.Forms;

namespace XDataTest
{
    public partial class Form1 : Form
    {
        private string rootUrl = "http://192.168.1.209/";
        private xfile Xfile { get; set; }
        public Form1()
        {
            InitializeComponent();
            SetTestData(); 
        }
        private void SetTestData()
        {
            xfile xf = new xfile();
            xf.wp_GUID = "e703ffdf-cdf9-4111-97ee-0747f531ebb2";
            xf.currency = "RMB";
            xf.beginMonth = "1";
            xf.customID = "8A2CB51F-5960-49D5-826F-4CACE35AD692";
            xf.customName = "合肥兴泰金融控股（集团）有限公司";
            xf.endMonth = "7";
            xf.fileName = "CwV114_001.34301_合肥圣达电子科技实业有限_2019.001";
            xf.pzBeginDate = "1/ 1";
            xf.pzEndDate = "7/ 5";
            xf.uploadTime = System.DateTime.Parse("2019 - 09 - 23 20:29:56.543");
            xf.uploadUser = "bj_hanhaiyan";
            xf.ztYear = "2019";
            xf.ztid = "{20190807}.001.34301";
            xf.dbName = "Test20";
            xf.projectID = xf.ztid;
            Xfile = xf;
        }
       
        private void button1_Click(object sender, EventArgs e)
        {
            string url = rootUrl+"xfile";             
            HttpGet(url,Xfile); 
        }
        public string HttpGet(string url,xfile xfile)
        {
            string strRet = "";
            HttpClientHandler httpHandler = new HttpClientHandler();
            httpHandler.AllowAutoRedirect = false;
            HttpClient httpClient = new HttpClient(httpHandler);
            try
            {
                var host = new System.Uri(url).Host;
                httpClient.DefaultRequestHeaders.Clear();
                //httpClient.DefaultRequestHeaders.Add("Host", host);
                httpClient.DefaultRequestHeaders.Add("Method", "GET");
                httpClient.DefaultRequestHeaders.Add("KeepAlive", "true");
                httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                var response = httpClient.GetAsync(url).Result;
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    string content = response.Content.ReadAsStringAsync().Result;
                    JsonSerializerOptions jsonSerializerOptions = new JsonSerializerOptions();
                    jsonSerializerOptions.PropertyNameCaseInsensitive = true;
                    var resultDict = JsonSerializer.Deserialize<dynamic>(content, jsonSerializerOptions);
                    var icount = 0;
                    foreach (var kv in resultDict.EnumerateArray())
                    {
                        var xf = JsonSerializer.Deserialize<xfile>(kv.ToString());
                        icount++;
                    }
                    MessageBox.Show(string.Format("共查到{0}条记录!",icount));
                }
            }
            catch (AggregateException ex)
            {
                foreach (var ee in ex.InnerExceptions)
                {
                    strRet += ee.Message + " ";
                }
            }
            catch (Exception ex)
            {
                strRet = ex.Message;
            }
            finally
            {
            }

            return strRet;
        }
        public void HttpHandlePost (string url, xfile xfile)
        {
            HttpClientHandler httpHandler = new HttpClientHandler();
            // httpHandler.AllowAutoRedirect = false;
            // System.Net.ServicePointManager.ServerCertificateValidationCallback += (send, certificate, chain, sslPolicyErrors) => { return true; };//这一句是强制忽略证书错误
            //httpHandler.ClientCertificateOptions = ClientCertificateOption.Automatic;
            // System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;
            string strRet = string.Empty;
            HttpClient httpClient = new HttpClient(httpHandler);   
            HttpContent postContent = null;
            HttpResponseMessage response = null;            
            try
            {
                var host = new System.Uri(url).Host;
                httpClient.DefaultRequestHeaders.Add("Host", "localhost");
                httpClient.DefaultRequestHeaders.Add("User-Agent", "PostmanRuntime/7.17.1");
                httpClient.DefaultRequestHeaders.Add("Accept", "*/*");
                httpClient.DefaultRequestHeaders.Add("Cache-Control", "no-cache");
                httpClient.DefaultRequestHeaders.Add("Accept-Encoding", "gzip, deflate");                
                httpClient.DefaultRequestHeaders.Add("Connection", "keep-alive");
                if (!(xfile == null || xfile.customID.Length ==0))
                {
                    
                   var  pjson = JsonSerializer.Serialize<xfile>(xfile);
                    //postContent = new FormUrlEncodedContent(body);
                    postContent = new StringContent(pjson, Encoding.UTF8, "application/json");
                    response = httpClient.PostAsync(url, postContent).Result;
                    if (response.StatusCode == System.Net.HttpStatusCode.OK)
                    {
                        string content = response.Content.ReadAsStringAsync().Result;
                        MessageBox.Show(content);
                        //var resultDict = JsonSerializer.Deserialize<Dictionary<string, string>>(content);
                         
                    }
                    else
                    {
                       
                    }
                }                  

                
            }
            catch (AggregateException ex)
            {
                foreach (var ee in ex.InnerExceptions)
                {
                    strRet += ee.Message + " ";
                }
            }
            catch (Exception ex)
            {
                strRet=  ex.Message;
            }
            finally
            {
                if (postContent != null)
                    postContent.Dispose();
                if (response != null)
                    response.Dispose();
            }
        }
        public static string HttpPost(string url, Dictionary<string, string> parameters)
        {
            //ServicePointManager.ServerCertificateValidationCallback += (send, certificate, chain, sslPolicyErrors) => { return true; };//这一句是强制忽略证书错误
            //httpHandler.ClientCertificateOptions = ClientCertificateOption.Automatic;
            //System.Net.ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls12 | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls;
            Encoding encoding = Encoding.UTF8;
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(url);
            request.ContentType = "application/json";
            request.Method = "POST";
            request.UserAgent = " PostmanRuntime/7.17.1";
            request.Accept = "*/*"; 
            request.ProtocolVersion = HttpVersion.Version11;
            if (!(parameters == null || parameters.Count == 0))
            {
                //StringBuilder buffer = new StringBuilder();
                //int i = 0;
                //foreach (string key in parameters.Keys)
                //{
                //    if (i > 0)
                //    {
                //        buffer.AppendFormat("&{0}={1}", key, parameters[key]);
                //    }
                //    else
                //    {
                //        buffer.AppendFormat("{0}={1}", key, parameters[key]);
                //    }
                //    i++;
                //}

                var pjson = JsonSerializer.Serialize<Dictionary<string, string>>(parameters);
                pjson = pjson.Replace("\"31\"","31");
                byte[] data = encoding.GetBytes(pjson.ToString());
                using (Stream stream = request.GetRequestStream())
                {
                    stream.Write(data, 0, data.Length);
                } 
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                using (StreamReader reader = new StreamReader(response.GetResponseStream(), Encoding.UTF8))
                {
                    return reader.ReadToEnd();
                }
            }
            return string.Empty;
           
        }

        private void button2_Click(object sender, EventArgs e)
        {
            string url = rootUrl+"XData/XData2SQL";
            Xfile.ztid = textBox1.Text.Trim();
            if (Xfile.ztid.Length == 0)
            {
                MessageBox.Show("账套ID不能为空");
            }
            //Dictionary<string, string> k2 = new Dictionary<string, string>();
            //k2.Add("FirstName", "Andrew");
            //k2.Add("LastName", "Lock");
            //k2.Add("Age","31");
            //HttpPost(url, k2);
            HttpHandlePost(url, Xfile);
        }

        private void button3_Click(object sender, EventArgs e)
        {
            string url = rootUrl+"XData/XData2EAS";
            HttpHandlePost(url, Xfile);

        }
    }
}
