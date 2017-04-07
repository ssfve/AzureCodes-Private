/*
 * 由SharpDevelop创建。
 * 用户： jianw
 * 日期: 2017/2/15
 * 时间: 19:31
 * 
 * 要改变这种模板请点击 工具|选项|代码编写|编辑标准头文件
 */
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Windows.Forms;
using System.Net;
using System.Threading;
using System.Text;
using System.IO;
//using System.Net.Json;
using Newtonsoft.Json;
using PostMan.Model;

namespace PostMan
{
	/// <summary>
	/// Description of MainForm.
	/// </summary>
	public partial class MainForm : Form
	{
        private object lockObj = new object();
        private Thread thread_rrinterval;
		private Thread thread_skinTemperature;
		private Thread thread_heartRate;
		private const string url = "http://16.250.1.189:25934/hpeiot4hc";
		private const int const_rrinterval = 1;
		private const int const_skinTemperature = 2;
		private const int const_heartRate = 3;
		
		public MainForm()
		{
			//
			// The InitializeComponent() call is required for Windows Forms designer support.
			//
			InitializeComponent();
			
			Control.CheckForIllegalCrossThreadCalls = false;
			
			

			//
			// TODO: Add constructor code after the InitializeComponent() call.
			//
		}
		void EnableButton()
		{
			bt_start.Enabled = !bt_start.Enabled;
			bt_stop.Enabled = !bt_stop.Enabled;
		}
		void AppendLog(string msg)
		{
			TB_info.AppendText(DateTime.Now.ToString() + ": \t" + msg + "\n");
		}
		void MainFormLoad(object sender, EventArgs e)
		{
			bt_start.Enabled = true;
			bt_stop.Enabled = false;
		}
		void Bt_startClick(object sender, EventArgs e)
		{
			EnableButton();
			
			//thread_rrinterval
			if (thread_rrinterval != null && thread_rrinterval.IsAlive) {
				AppendLog("thread_rrinterval already running, the thread will abort");
			} else {
				thread_rrinterval = new Thread(Thread_RRinterval);
				thread_rrinterval.Start();
                //thread_rrinterval.IsBackground = true;
                AppendLog("Start thread_rrinterval");
			}
			
			//thread_skinTemperature
			if (thread_skinTemperature != null && thread_skinTemperature.IsAlive) {
				AppendLog("thread_skinTemperature already running, the thread will abort");
			} else {
				thread_skinTemperature = new Thread(Thread_SkinTemperature);
				thread_skinTemperature.Start();
				AppendLog("Start thread_skinTemperature");
			}
			
			//thread_heartRate
			if (thread_heartRate != null && thread_heartRate.IsAlive) {
				AppendLog("thread_heartRate already running, the thread will abort");
			} else {
				thread_heartRate = new Thread(Thread_HeartRate);
				thread_heartRate.Start();
				AppendLog("Start thread_heartRate");
			}
			
		}
		void Bt_stopClick(object sender, EventArgs e)
		{
			EnableButton();
			AbortThread();
		}
		void MainFormFormClosed(object sender, FormClosedEventArgs e)
		{
			AbortThread();
		}
		
		private void AbortThread()
		{
			if (thread_rrinterval != null) {
				if (thread_rrinterval.IsAlive) {
					thread_rrinterval.Abort();
					AppendLog("thread_rrinterval aborted");
				}
			}
			if (thread_skinTemperature != null) {
				if (thread_skinTemperature.IsAlive) {
					thread_skinTemperature.Abort();
					AppendLog("thread_skinTemperature aborted");
				}
			}
			if (thread_heartRate != null) {
				if (thread_heartRate.IsAlive) {
					thread_heartRate.Abort();
					AppendLog("thread_heartRate aborted");
				}
			}
		}
		
		private void Thread_RRinterval()
		{
			WebRequestProcess(const_rrinterval);
		}
		private void Thread_HeartRate()
		{
			WebRequestProcess(const_heartRate);
		}
		private void Thread_SkinTemperature()
		{
			WebRequestProcess(const_skinTemperature);
		}
		
		private void WebRequestProcess(int Models)
		{
			string[] JsonObjects = null;
			switch (Models) {
				case const_rrinterval:
					JsonObjects = File.ReadAllLines(Application.StartupPath + "\\rrinterval.txt");
					break;
				case const_skinTemperature:
					JsonObjects = File.ReadAllLines(Application.StartupPath + "\\skinTemperature.txt");
					break;
				case const_heartRate:
					JsonObjects = File.ReadAllLines(Application.StartupPath + "\\heartRate.txt");
					break;
			}
			
			foreach (var json in JsonObjects) {
				try {
					switch (Models) {
						case const_rrinterval:
                            rrinterval ri = JsonConvert.DeserializeObject<rrinterval>(json);
							ri.timestamp = DateTime.UtcNow.Ticks / 10000 - (new DateTime(1970, 1, 1)).Ticks / 10000;
							HttpPostData(JsonConvert.SerializeObject(ri));
							break;
						case const_skinTemperature:
                            skinTemperature st = JsonConvert.DeserializeObject<skinTemperature>(json);
							st.timestamp = DateTime.UtcNow.Ticks / 10000 - (new DateTime(1970, 1, 1)).Ticks / 10000;
							HttpPostData(JsonConvert.SerializeObject(st));
							break;
						case const_heartRate:
							heartRate hr = JsonConvert.DeserializeObject<heartRate>(json);
							hr.timestamp = DateTime.UtcNow.Ticks / 10000 - (new DateTime(1970, 1, 1)).Ticks / 10000;
							HttpPostData(JsonConvert.SerializeObject(hr));
							break;
					}
				} catch (Exception e) {
					MessageBox.Show(e.ToString());
				}
			}
			
			
			#region use for Dictionary
//			while (true) {
//				try {
//					Dictionary<string, string> dir = new Dictionary<string, string>();
//					dir.Add("Id", count.ToString());
//					dir.Add("Value", DateTime.Now.ToString());
//					HttpPostData("http://16.250.1.189:25934/hpeiot4hc", dir);
//					dir = null;
//					Thread.Sleep(1000);
//					count++;
//				} catch (Exception e) {
//					MessageBox.Show(e.ToString());
//				}
//			}
			#endregion
		}
		private void HttpPostData(string postData)
		{
            lock (lockObj)  
            {
                AppendLog(postData);
                HttpWebRequest request;
                System.Net.ServicePointManager.DefaultConnectionLimit = 200;
                System.GC.Collect();
                request = (HttpWebRequest)HttpWebRequest.Create(url);
                request.Method = "POST";
                request.ContentType = "application/json";
                request.KeepAlive = false;

                //1000ms = 1s timeout after 1s
                request.Timeout = 30000;
                byte[] data;
                data = System.Text.Encoding.UTF8.GetBytes(postData);
                request.ContentLength = data.Length;
                Stream writer = request.GetRequestStream();
                writer.Write(data, 0, data.Length);
                writer.Close();
                writer = null;

                AppendLog("msg sent returned successfully");

                HttpWebResponse httpResponse = (HttpWebResponse)request.GetResponse();
                Stream stmResponse = httpResponse.GetResponseStream();
                stmResponse.Close();
                stmResponse = null;
            }
            //if (resp != null)
            //{
            //  resp.Close();
            //}
            //if (req != null)
            //{
            //  req.Abort();
            //}
            //			System.Net.HttpWebResponse  response;
            //      
            //			response  =  (System.Net.HttpWebResponse)request.GetResponse();
            //      
            //			System.IO.Stream  s;
            //      
            //			s  =  response.GetResponseStream();
            //      
            //			string  StrDate  =  "";
            //      
            //			string  strValue  =  "";
            //      
            //			StreamReader  Reader  =  new StreamReader(s,  Encoding.UTF8);
            //      
            //			while  ((StrDate  =  Reader.ReadLine())  !=  null) {
            //          
            //				strValue  +=  StrDate  +  "\r\n";
            //			}
            //AppendLog(strValue);
        }

        // Post Data to Server
        private void HttpPostData(Dictionary<string, string> postData)
		{
			HttpWebRequest request;
			request = (HttpWebRequest)HttpWebRequest.Create(url);
			request.Method = "POST";
			request.ContentType  =  "application/json;charset=UTF-8";
			
			string mParams = "";
			bool firstParam = true;
			
			foreach (var param in postData) {
				if (firstParam) {
					firstParam = false;
				} else {
					mParams += "&";
				}
				mParams += param.Key + "=" + Uri.EscapeDataString(param.Value);
			}
			
			byte[]  data;
			data = System.Text.Encoding.UTF8.GetBytes(mParams);
			request.ContentLength = data.Length;
			Stream  writer  =  request.GetRequestStream();
			writer.Write(data,  0,  data.Length);
			writer.Close();
			AppendLog(mParams);
		}

	}
}

