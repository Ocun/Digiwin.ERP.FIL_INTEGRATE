//---------------------------------------------------------------- 
//Copyright (C) 2010-2012 Digital China Management System Co.,Ltd
//Http://www.Dcms.com.cn 
// All rights reserved.
//<author>zhangcn</author>
//<createDate>2016-11-16</createDate>
//<description>更新实盘数量 服务实现</description>
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Data;
using System.Linq;
using Digiwin.Common;
using Digiwin.Common.Core;
using Digiwin.Common.Query2;
using Digiwin.Common.Services;
using Digiwin.Common.Torridity.Metadata;
using Digiwin.ERP.Common.Business;
using Digiwin.ERP.Common.Utils;
using Digiwin.Common.Torridity;

namespace Digiwin.ERP.FIL_INTEGRATE.Business.Implement{
    /// <summary>
    ///更新实盘数量 服务
    /// </summary>
    [ServiceClass(typeof (IUpdateCountingService))]
    [Description("更新实盘数量 服务")]
    public class UpdateCountingService : ServiceComponent, IUpdateCountingService{

    }
}
