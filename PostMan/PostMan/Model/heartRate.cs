/*
 * Created by SharpDevelop.
 * User: luoj
 * Date: 2017/2/16
 * Time: 15:03
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Runtime.Serialization;
namespace PostMan.Model
{
	/// <summary>
	/// Description of Class1.
	/// </summary>
	[DataContract]
	public class heartRate
	{
		public heartRate()
		{
		}
		[DataMember]  
		public string eventtype { get; set; }
		[DataMember]  
		public string userid { get; set; }
		[DataMember]  
		public int rate { get; set; }
		[DataMember]  
		public string deviceid { get; set; }
		[DataMember]  
		public long timestamp { get; set; }
		[DataMember]  
		public string trainingMode { get; set; }
		[DataMember]  
		public int quality { get; set; }
	}
}
