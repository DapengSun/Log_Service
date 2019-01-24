using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace SoftActtive.Models.Kafka
{
    public class SoftInfoOn
    {
        public string Id { get; set; }

        /// <summary>
        /// 软件类别编号
        /// </summary>
        public int Mtype { get; set; }
        /// <summary>
        /// 软件类别
        /// </summary>
        public string MtypeName { get; set; }
        /// <summary>
        /// 软件id，如CAXA电子图板-标准版对应1002
        /// </summary>
        public int ProdId { get; set; }
        /// <summary>
        /// 软件名称
        /// </summary>
        public string Prodname { get; set; }
        /// <summary>
        /// 公网IP
        /// </summary>
        public string IP { get; set; }
        /// <summary>
        /// 内网IP
        /// </summary>
        public string InnerIP { get; set; }
        /// <summary>
        /// 省
        /// </summary>
        public string Provice { get; set; }
        /// <summary>
        /// 市
        /// </summary>
        public string City { get; set; }
        /// <summary>
        /// 内部版本
        /// </summary>
        public string VerId { get; set; }
        /// <summary>
        /// 版本
        /// </summary>
        public string EndId { get; set; }
        /// <summary>
        /// 加密锁号
        /// </summary>
        public string EndNo { get; set; }
        /// <summary>
        /// 用户计算机标识号
        /// </summary>
        public string PcHard { get; set; }
        /// <summary>
        /// 用户计算机名称
        /// </summary>
        public string PcName { get; set; }
        /// <summary>
        /// 客户端时间
        /// </summary>
        public DateTime ClientTime { get; set; }
        /// <summary>
        /// 许可类型（ 0-试用；1-单机；2-网络；3-软加密；4-在线;-1-未知，其中3-软加密是国外独用，我们没有）
        /// </summary>
        public int LicenseType { get; set; }
        /// <summary>
        /// 用户id
        /// </summary>
        public string AccountId { get; set; }
        /// <summary>
        /// 用户登录名，一般对应手机号
        /// </summary>
        public string Loginname { get; set; }
        /// <summary>
        /// 用户名，一般对应公司名称和个人姓名
        /// </summary>
        public string custname { get; set; }
        /// <summary>
        /// 操作系统
        /// </summary>
        public string OS { get; set; }
        /// <summary>
        /// 创建时间
        /// </summary>
        public DateTime CDate { get; set; }
        /// <summary>
        /// 数据删除状态
        /// </summary>
        public int SysStatus { get; set; }
        /// <summary>
        /// 显卡
        /// </summary>
        public string GraCard { get; set; }
        /// <summary>
        /// 操作系统类型
        /// </summary>
        public string OSType { get; set; }

        public SoftInfoOn()
        {
            Id = Guid.NewGuid().ToString();
            CDate = DateTime.Now;
            SysStatus = 0;
        }
    }
}