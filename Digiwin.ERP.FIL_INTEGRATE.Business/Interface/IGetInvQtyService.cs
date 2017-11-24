//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/14 13:49:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>根据提供的工厂、品号等信息，获取对应的现有库存量</description>
//20170515 modi by liwei1 for P001-170420001 增加重置接口，只存在参数site_no、show_zero_inventory、scan_warehouse_no、scan_storage_spaces_no

using System.Collections;
using System.ComponentModel;
using Digiwin.Common;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    [Description("根据提供的工厂、品号等信息，获取对应的现有库存量")]
    public interface IGetInvQtyService {
        /// <summary>
        /// 根据提供的工厂、品号等信息，获取对应的现有库存量
        /// </summary>
        /// <param name="site_no">工厂</param>
        /// <param name="show_zero_inventory">显示零库存</param>
        /// <param name="scan_warehouse_no">扫描仓库</param>
        /// <param name="scan_storage_spaces_no">扫描储位</param>
        /// <returns></returns>
        Hashtable GetInvQty(string site_no, string show_zero_inventory, string scan_warehouse_no, string scan_storage_spaces_no);//20170515 add by liwei1 for P001-170420001

        /// <summary>
        /// 根据提供的工厂、品号等信息，获取对应的现有库存量
        /// </summary>
        /// <param name="site_no">工厂</param>
        /// <param name="show_zero_inventory">显示零库存</param>
        /// <param name="scan_barcode">扫描条形码</param>
        /// <param name="scan_warehouse_no">扫描仓库</param>
        /// <param name="scan_storage_spaces_no">扫描储位</param>
        /// <returns></returns>
        Hashtable GetInvQty(string site_no, string show_zero_inventory, string scan_barcode, string scan_warehouse_no,
            string scan_storage_spaces_no);

        /// <summary>
        /// 根据提供的工厂、品号等信息，获取对应的现有库存量
        /// </summary>
        /// <param name="site_no">工厂</param>
        /// <param name="show_zero_inventory">显示零库存</param>
        /// <param name="scan_barcode">扫描条形码</param>
        /// <param name="scan_warehouse_no">扫描仓库</param>
        /// <param name="scan_storage_spaces_no">扫描储位</param>
        /// <param name="program_job_no">作业编号：15库存查询 18条码冻结</param>
        /// <returns></returns>
        Hashtable GetInvQty(string site_no, string show_zero_inventory, string scan_barcode, string scan_warehouse_no,
            string scan_storage_spaces_no, string program_job_no);//20170329 add by wangrm for P001-170316001
    }
}
