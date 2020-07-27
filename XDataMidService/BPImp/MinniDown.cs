using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using System.Web;

/// <summary>
/// author : bj_hhy
/// </summary>
namespace XDataMidService.BPImp
{
    public class MinniDown
    {
      
        HttpClient httpClient = null;
        private readonly object dlock = new object();
        string _host, _logName, _logPwd,_token;
        public MinniDown(string host,string logName,string logPwd)
        {
            _host = host;
            _logName = logName;
            _logPwd = logPwd;
            httpClient = new  HttpClient();
            httpClient.Timeout = TimeSpan.FromMinutes(15);
            GetToken(out _token);
        }
       // [MethodImpl(MethodImplOptions.Synchronized)]
        public bool DownloadFile(string libID, string libFilePathName, string localFilePathName, out string strRet)
        {
            bool bRet = false;
            strRet = "";

            bRet = GetDownloadURL(libID, libFilePathName, false, out strRet);
            if (bRet == false)
            {
                return false;
            }
            UriBuilder uriBuilder = new UriBuilder(strRet);
            string tmpHost = uriBuilder.Host;
            string downloadURL = strRet.Replace(tmpHost,_host);
            strRet = "";

            bRet = DownloadFile(downloadURL, localFilePathName, out strRet);

            return bRet;
        } public bool GetDownloadURL(string libID, string libFilePathName, bool bReuse, out string strRet)
        {
            bool bRet = false;
            strRet = "";

            HttpResponseMessage response = null;

            try
            {
                if (libFilePathName.StartsWith("/"))
                {
                    libFilePathName = libFilePathName.Substring(1);
                }

                httpClient.DefaultRequestHeaders.Clear();
                httpClient.DefaultRequestHeaders.Add("Authorization", "Token " + _token);
                httpClient.DefaultRequestHeaders.Add("Accept", "application/json; indent=4");
                httpClient.DefaultRequestHeaders.Add("KeepAlive", "true");

                string strReuse = null;
                if (bReuse == true)
                {
                    strReuse = "&reuse=1";
                }
                string requestURI = "http://" + _host + "/api2/repos/" + libID + "/file/?p=/" + HttpUtility.UrlEncode(libFilePathName, Encoding.UTF8) + strReuse;

                response = httpClient.GetAsync(requestURI).Result;
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    string content = response.Content.ReadAsStringAsync().Result;
                    strRet = content.Replace("\"", "");
                    bRet = true;
                }
                else
                {
                    strRet = response.StatusCode.ToString();
                }
            }
            catch (AggregateException ex)
            {
                foreach (var e in ex.InnerExceptions)
                {
                    strRet += e.Message + " ";
                }
            }
            catch (Exception ex)
            {
                strRet = ex.Message;
            }
            finally
            {
                if (response != null)
                    response.Dispose();
            }

            return bRet;
        }
        public bool DownloadFile(string downloadURL, string localFilePathName, out string strRet)
        {
            bool bRet = false;
            strRet = "";

            FileStream fs = null;
            Stream stream = null;
            HttpResponseMessage response = null;

            try
            {
                fs = new FileStream(localFilePathName, FileMode.Create);

                httpClient.DefaultRequestHeaders.Clear();
                httpClient.DefaultRequestHeaders.Add("Authorization", "Token " + _token);
                httpClient.DefaultRequestHeaders.Add("Accept", "application/json; indent=4");
                httpClient.DefaultRequestHeaders.Add("KeepAlive", "true");               
                response = httpClient.GetAsync(downloadURL).Result;
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    stream = response.Content.ReadAsStreamAsync().Result;
                    stream.CopyTo(fs);
                    fs.Flush();

                    bRet = true;
                }
                else
                {
                    strRet = response.StatusCode.ToString();
                }
            }
            catch (AggregateException ex)
            {
                foreach (var e in ex.InnerExceptions)
                {
                    strRet += e.Message + " ";
                }
            }
            catch (Exception ex)
            {
                strRet = ex.Message;
            }
            finally
            {
                if (fs != null)
                {
                    fs.Flush();
                    fs.Dispose();
                }
                if (stream != null)
                    stream.Dispose();
                if (response != null)
                    response.Dispose();
            }

            return bRet;
        }


        private bool GetToken(out string strRet)
        {
            bool bRet = false;
            strRet = ""; 
            HttpContent postContent = null;
            HttpResponseMessage response = null;

            try
            {
                httpClient.DefaultRequestHeaders.Clear();
                httpClient.DefaultRequestHeaders.Add("Host",_host);
                httpClient.DefaultRequestHeaders.Add("Method", "Post");
                httpClient.DefaultRequestHeaders.Add("KeepAlive", "true");

                postContent = new FormUrlEncodedContent(new Dictionary<string, string>()
                {
                    {"username",_logName},
                    {"password", _logPwd}
                });

                response = httpClient.PostAsync("http://" + _host + "/api2/auth-token/", postContent).Result;
                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    string content = response.Content.ReadAsStringAsync().Result;
                    Dictionary<string, string> resultDict = Newtonsoft.Json.JsonConvert.DeserializeObject<Dictionary<string, string>>(content);
                    if (resultDict.ContainsKey("token"))
                    {
                        strRet = resultDict["token"];
                        bRet = true;
                    }
                    else
                    {
                        strRet = content;
                    }
                }
                else
                {
                    strRet = response.StatusCode.ToString();
                }
            }
            catch (AggregateException ex)
            {
                foreach (var e in ex.InnerExceptions)
                {
                    strRet += e.Message + " ";
                }
            }
            catch (Exception ex)
            {
                strRet = ex.Message;
            }
            finally
            {
                if (postContent != null)
                    postContent.Dispose();
                if (response != null)
                    response.Dispose();
            }

            return bRet;
        }
    }
}
