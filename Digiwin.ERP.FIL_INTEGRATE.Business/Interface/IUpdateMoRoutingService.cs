//---------------------------------------------------------------- 
//Copyright (C) 2005-2006 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
//All rights reserved.
//<author>ShenBao</author>
//<createDate>2016/11/16 10:09:37</createDate>
//<IssueNo>P001-161101002</IssueNo>
//<description>更新工单工艺信息服务接口</description>
//20170406 modi by wangyq for P001-170327001 +补上lot_no，其他比规格缺少的字段跟sd确认后是无用的,无需添加

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Digiwin.Common;
using Digiwin.Common.Torridity;
using System.Collections;

namespace Digiwin.ERP.FIL_INTEGRATE.Business {
    [TypeKeyOnly]
    public interface IUpdateMoRoutingService {
        /// <summary>
        /// 更新工单工艺信息
        /// </summary>
        /// <param name="acct">员工编号</param>
        /// <param name="report_type">报工类别</param>
        /// <param name="wo_no">工单号码</param>
        /// <param name="run_card_no">Run Card</param>
        /// <param name="op_no">作业编号</param>
        /// <param name="op_seq">作业序</param>
        /// <param name="workstation_no">工作站</param>
        /// <param name="machine_no">机器编号</param>
        /// <param name="shift_no">报工班别</param>
        /// <param name="labor_hours">工时</param>
        /// <param name="machine_hours">机时</param>
        /// <param name="reports_qty">报工数量</param>
        /// <param name="scrap_qty">报废数量</param>
        /// <param name="item_no">生产料号</param>
        /// <param name="site_no">营运据点</param>
        /// <returns></returns>
        Hashtable UpdateMoRouting(string site_no, string report_type, string wo_no, string run_card_no
            , string op_no, string op_seq, string workstation_no, string machine_no
            , int labor_hours, int machine_hours, decimal reports_qty
            , decimal scrap_qty, string item_no, string shift_no, string employee_no, string employee_name
            , string lot_no, string warehouse_no, string storage_spaces_no//20170406 add by wangyq for P001-170327001 先补上lot_no，其他比规格缺少的字段跟sd确认后是无用的,无需添加
            );
    }
}
