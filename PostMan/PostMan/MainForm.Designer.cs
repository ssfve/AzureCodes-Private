/*
 * 由SharpDevelop创建。
 * 用户： jianw
 * 日期: 2017/2/15
 * 时间: 19:31
 * 
 * 要改变这种模板请点击 工具|选项|代码编写|编辑标准头文件
 */
namespace PostMan
{
	partial class MainForm
	{
		/// <summary>
		/// Designer variable used to keep track of non-visual components.
		/// </summary>
		private System.ComponentModel.IContainer components = null;
		private System.Windows.Forms.Button bt_start;
		private System.Windows.Forms.RichTextBox TB_info;
		private System.Windows.Forms.Button bt_stop;
		
		/// <summary>
		/// Disposes resources used by the form.
		/// </summary>
		/// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
		protected override void Dispose(bool disposing)
		{
			if (disposing) {
				if (components != null) {
					components.Dispose();
				}
			}
			base.Dispose(disposing);
		}
		
		/// <summary>
		/// This method is required for Windows Forms designer support.
		/// Do not change the method contents inside the source code editor. The Forms designer might
		/// not be able to load this method if it was changed manually.
		/// </summary>
		private void InitializeComponent()
		{
			System.ComponentModel.ComponentResourceManager resources = new System.ComponentModel.ComponentResourceManager(typeof(MainForm));
			this.bt_start = new System.Windows.Forms.Button();
			this.TB_info = new System.Windows.Forms.RichTextBox();
			this.bt_stop = new System.Windows.Forms.Button();
			this.SuspendLayout();
			// 
			// bt_start
			// 
			this.bt_start.Location = new System.Drawing.Point(28, 13);
			this.bt_start.Name = "bt_start";
			this.bt_start.Size = new System.Drawing.Size(157, 25);
			this.bt_start.TabIndex = 0;
			this.bt_start.Text = "Start Post Data";
			this.bt_start.UseVisualStyleBackColor = true;
			this.bt_start.Click += new System.EventHandler(this.Bt_startClick);
			// 
			// TB_info
			// 
			this.TB_info.BackColor = System.Drawing.SystemColors.ControlDark;
			this.TB_info.BorderStyle = System.Windows.Forms.BorderStyle.None;
			this.TB_info.ForeColor = System.Drawing.SystemColors.Info;
			this.TB_info.Location = new System.Drawing.Point(28, 57);
			this.TB_info.Name = "TB_info";
			this.TB_info.ReadOnly = true;
			this.TB_info.Size = new System.Drawing.Size(346, 278);
			this.TB_info.TabIndex = 1;
			this.TB_info.Text = "";
			// 
			// bt_stop
			// 
			this.bt_stop.Location = new System.Drawing.Point(217, 13);
			this.bt_stop.Name = "bt_stop";
			this.bt_stop.Size = new System.Drawing.Size(157, 25);
			this.bt_stop.TabIndex = 2;
			this.bt_stop.Text = "Stop Post Data";
			this.bt_stop.UseVisualStyleBackColor = true;
			this.bt_stop.Click += new System.EventHandler(this.Bt_stopClick);
			// 
			// MainForm
			// 
			this.AutoScaleDimensions = new System.Drawing.SizeF(6F, 13F);
			this.AutoScaleMode = System.Windows.Forms.AutoScaleMode.Font;
			this.ClientSize = new System.Drawing.Size(400, 349);
			this.Controls.Add(this.bt_stop);
			this.Controls.Add(this.TB_info);
			this.Controls.Add(this.bt_start);
			this.Icon = ((System.Drawing.Icon)(resources.GetObject("$this.Icon")));
			this.MaximizeBox = false;
			this.Name = "MainForm";
			this.StartPosition = System.Windows.Forms.FormStartPosition.CenterScreen;
			this.Text = "PostMan";
			this.FormClosed += new System.Windows.Forms.FormClosedEventHandler(this.MainFormFormClosed);
			this.Load += new System.EventHandler(this.MainFormLoad);
			this.ResumeLayout(false);

		}
	}
}
