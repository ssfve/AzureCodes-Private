/*
 * Created by SharpDevelop.
 * User: luoj
 * Date: 2017/2/16
 * Time: 15:05
 * 
 * To change this template use Tools | Options | Coding | Edit Standard Headers.
 */
using System;
using System.Runtime.Serialization;
namespace PostMan.Model
{
	/// <summary>
	/// Description of rrinterval.
	/// </summary>
	/// 
	[DataContract]
	public class rrinterval
	{
		public rrinterval()
		{
		}
		
		[DataMember]
		public string eventtype { get; set; }
		[DataMember]
		public decimal interval { get; set; }
		[DataMember]  
		public string userid { get; set; }
		[DataMember]  
		public string deviceid { get; set; }
		[DataMember]  
		public long timestamp { get; set; }
		[DataMember]  
		public string trainingMode { get; set; }
	}
}
