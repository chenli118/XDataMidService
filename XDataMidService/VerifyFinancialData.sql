 
IF EXISTS (SELECT * FROM sysobjects WHERE type = 'P' AND name = 'VerifyFinancialData')
	BEGIN
		DROP  Procedure  VerifyFinancialData
	END

GO
/***********
Project :  导入财务数据校验
errortype : 1-提示；2-警告；3-错误

***********/

CREATE Procedure VerifyFinancialData(@XID int)
AS

begin try

SET NOCOUNT ON 

if OBJECT_ID('tempdb..#KMXMTYPE ') IS NOT NULL    
drop table #KMXMTYPE  

if OBJECT_ID('tempdb..#LSTAB ') IS NOT NULL    
drop table  #LSTAB 

delete from xdata..facheck where xid = @XID

if not exists(select * from sys.objects where name = 'km')
BEGIN
insert into xdata..facheck
values(@XID,'企业无财务数据，请核实原因后重新采集！',3,1)
return
END

IF  EXISTS(SELECT 1 FROM xdata..xfiles WHERE XID = @XID AND MountType LIKE 'SAP %')
BEGIN
insert into xdata..facheck
values(@XID,'财务数据校验通过，允许导入数据',1,100)
return
END

--1 财务数据可审性校验
IF not exists(select * from sys.objects where name = 'jzpz') AND not exists(select * from sys.objects where name = 'kmye') 

BEGIN
insert into xdata..facheck
values(@XID,'企业期初余额数据，本期凭证数据均不存在；',3,1)
return
END

IF not exists(select * from sys.objects where name = 'jzpz') and exists (select 1 from kmye  having max(ncye)=0 and MIN(Ncye)=0)
begin
insert into xdata..facheck
values(@XID,'企业期初余额数据，本期凭证数据均不存在；',3,1)
return
end

--2 检查空名称的会计科目（找kmdm）
declare @number nvarchar(max)
if exists(select kmdm from km where (Kmmc is null or len(Kmmc)<1))
begin
set @number = (select kmdm + ' ;' from km where (Kmmc is null or len(Kmmc)<1) for xml path(''))

insert into xdata..facheck
values(@XID,'以下会计科目代码的名称为空：'+@number,3,2)
end

--3 未定义会计科目
IF exists(select * from sys.objects where name = 'kmye')
Begin

declare @tbd nvarchar(max)
if exists (select k1.Kmdm from kmye k1 where not exists(select 1 from km  where k1.Kmdm = km.Kmdm))
begin 
set @tbd =(SELECT kmdm +';' from 
(select distinct k1.Kmdm AS kmdm from kmye k1 where not exists(select 1 from km  where k1.Kmdm = km.Kmdm))A for xml path(''))

insert into  xdata..facheck
values(@XID,'以下会计科目没有在财务系统中设置，但存在于期初余额中:'+ @tbd,3,3)
end 

--6 检验期初余额是否平衡
declare @ncye decimal(19, 3)
select @ncye=sum(ncye) from kmye where  ismx=1
if @ncye <> 0
begin
insert into  xdata..facheck
values(@XID,'企业财务数据期初余额不平；',3,6)
end

--8 科目余额唯一性校验
declare @subRepeat nvarchar(max)
if exists(select Kmdm from kmye k1 group by Kmdm having count(*) >1)
begin
set @subRepeat =
(select '['+sub+']; ' from 
(select '科目编码：' + kmdm + '; 科目名称：' + (select kmmc from km where k1.kmdm = kmdm) as sub from kmye k1 
group by Kmdm having count(*) >1)A for xml path(''))

insert into xdata..facheck
values(@XID,'存在重复的科目余额：' +@subRepeat,1,8)
end 

End

-- 凭证表校验
IF exists(select * from sys.objects where name = 'jzpz')
Begin
declare @undefinedvoucher nvarchar(max)=''
declare @jfje decimal(19,3)=0
declare @dfje decimal(19,3)=0
declare @fx varchar(max)=''
declare @fgf char(1)
declare @yc int =0
declare @final nvarchar(max)=''
declare @leakage nvarchar(max)=''

declare @subitem table(
ID int identity(1,1),
pzbh_date nvarchar(1000),
jd nvarchar(10),
rmb decimal(19,3)
)

declare @nonfinal table
(
pzh nvarchar(1000),
pz_date nvarchar(1000)
)

declare @succession table
(
 xh bigint,
 pzh nvarchar(max) ,
 pzfl varchar(100),
 months varchar(10)
)

--未定义
if exists (select j1.Kmdm from jzpz j1 where not exists(select 1 from km where j1.Kmdm = km.Kmdm))
begin
set @undefinedvoucher = (SELECT kmdm + ';' from 
(select distinct j1.Kmdm AS kmdm from jzpz j1 where not exists(select 1 from km  where j1.Kmdm = km.Kmdm))A for xml path(''))
end 

--总额平衡
select @jfje= sum(rmb) from jzpz where Jd = '借'
select @dfje= sum(rmb) from jzpz where jd = '贷'

--分项平衡
insert into @subitem(pzbh_date,jd,rmb)
select Pzh+','+pz_date,Jd,case when Jd = '借' then Rmb else (-1)*Rmb end
 from jzpz

if exists(select 1 from @subitem group by pzbh_date having sum(rmb) <> 0)begin 
 set @fx = (select '['+a+']; ' from 
(select '凭证编号：'+stuff(pzbh_date,CHARINDEX(',',pzbh_date)+1,0,' 凭证日期：') AS a from @subitem  group by pzbh_date having sum(rmb) <> 0)
A for xml path(''))
end

--日期异常

 if exists(select 1 from jzpz where ISDATE(Pz_Date) = 0)
 begin
 set @yc= 1
end

--连续
insert into @succession(xh,pzh,pzfl,months)
select xh,pzh,fl,months from
(select distinct cast(xdata.dbo.f_GetNum(Pzbh) as bigint)as xh,Pzh,isnull(Pzlx_Dm,'')+isnull(Pzlx_Mc,'') as fl,
MONTH(convert(datetime,Pz_Date)) as months from jzpz)PZ  order by months,fl

set @leakage = (
select dh + ';' from (
select months+'月凭证['+ pzh+']与['+(select pzh from @succession where T.later = xh and T.months = months and T.pzfl = pzfl)+']之间存在断号' as dh from 
(select xh as front ,(select min(xh) from @succession where s.xh <xh and s.months = months and s.pzfl = pzfl)as later,pzh,months,pzfl 
from @succession s )T where later-front >1 )A  order by dh for xml path(''))

--非末级
insert into @nonfinal
select j1.Pzh,j1.Pz_Date from jzpz j1 left join km k1 on j1.kmdm = k1.kmdm where k1.Kmmx <>1

if exists(select 1 from @nonfinal)
begin
set @final = (SELECT b + ' ;' from(select '凭证号：' +pzh + ' ,凭证日期：' + pz_date as b from @nonfinal)A for xml path(''))
end

if exists(select * from sys.objects where name = 'pzk') and exists(select 1 from pzk having count(*)>1)
BEGIN
--创建临时表
if OBJECT_ID('tempdb..#jzpzTemp ') IS NOT NULL    
drop table #jzpzTemp

create table #jzpzTemp
(
incno nvarchar(1000),
pzh nvarchar(1000),
Pzbh nvarchar(1000),
jd  char(2),
kmdm nvarchar(1000) collate Chinese_PRC_CS_AS_KS_WS,
rmb decimal(19, 3),
Pzlx_Dm nvarchar(1000),
Pzlx_Mc nvarchar(1000),
pz_date nvarchar(1000)
)

--声明游标
declare @customerID nvarchar(50)
declare cusCursor cursor for
select pzk_tablename from pzk where Pzk_TableName collate Chinese_PRC_CS_AS_KS_WS like 'jzpz%'
open cusCursor
fetch next from cusCursor into @customerID
while(@@FETCH_STATUS = 0)
begin
exec(
'insert into #jzpzTemp select incno,pzh,pzbh,jd,kmdm,rmb,pzlx_dm,pzlx_mc,pz_date from ' +@customerID
)
delete from @subitem
delete from @nonfinal
delete from @succession

-- 未定义
if exists (select j1.Kmdm from #jzpzTemp j1 where not exists(select 1 from km where j1.Kmdm collate Chinese_PRC_CS_AS_KS_WS= km.Kmdm))
begin
set @undefinedvoucher = @undefinedvoucher+(SELECT kmdm + ';' from 
(select distinct j1.Kmdm AS kmdm from #jzpzTemp j1 where not exists(select 1 from km  where j1.Kmdm collate Chinese_PRC_CS_AS_KS_WS= km.Kmdm))A for xml path(''))
end 

-- 总额平衡
select @jfje=@jfje + sum(rmb) from #jzpzTemp where Jd = '借'
select @dfje=@dfje + sum(rmb) from #jzpzTemp where jd = '贷'

--分项平衡
insert into @subitem(pzbh_date,jd,rmb)
select Pzh+','+pz_date,Jd,case when Jd = '借' then Rmb else (-1)*Rmb end
 from #jzpzTemp

if exists(select 1 from @subitem group by pzbh_date having sum(rmb) <> 0)begin 
 set @fx =@fx+ (select '['+a+']; ' from 
(select '凭证编号：'+stuff(pzbh_date,CHARINDEX(',',pzbh_date)+1,0,' 凭证日期：') AS a from @subitem  group by pzbh_date having sum(rmb) <> 0)
A for xml path(''))
end

--日期异常
 if exists( select 1 from jzpz where ISDATE(Pz_Date) = 0)
 begin
  set @yc= 1
end

--编号连续性
insert into @succession(xh,pzh,pzfl,months)
select xh,pzh,fl,months from
(select distinct cast(xdata.dbo.f_GetNum(Pzbh) as bigint)as xh,Pzh,isnull(Pzlx_Dm,'')+isnull(Pzlx_Mc,'') as fl,
MONTH(convert(datetime,Pz_Date)) as months from #jzpzTemp)PZ  order by months,fl

set @leakage =@leakage + (
select dh + ';' from (
select months+'月凭证['+ pzh+']与['+(select pzh from @succession where T.later = xh and T.months = months and T.pzfl = pzfl)+']之间存在断号' as dh from 
(select xh as front ,(select min(xh) from @succession where s.xh <xh and s.months = months and s.pzfl = pzfl)as later,pzh,months,pzfl 
from @succession s )T where later-front >1 )A  order by dh for xml path(''))

--非末级科目
insert into @nonfinal
select j1.Pzh,j1.Pz_Date from #jzpzTemp j1 left join km k1 on j1.kmdm collate Chinese_PRC_CS_AS_KS_WS= k1.kmdm where k1.Kmmx <>1

if exists(select 1 from @nonfinal)
begin
set @final =@final+ (SELECT b + ' ;' from(select '凭证号：' +pzh + ' ,凭证日期：' + pz_date as b from @nonfinal)A for xml path(''))
end

delete from #jzpzTemp
fetch next from cusCursor into @customerID
end
close cusCursor
deallocate  cusCursor
END
 

IF len(@undefinedvoucher) >0 
begin
insert into  xdata..facheck
values(@XID,'以下会计科目没有在财务系统中设置，但存在凭证表中:'+ @undefinedvoucher,3,3)
end

IF @jfje-@dfje <> 0
begin
 insert into xdata..facheck
 values(@XID,'凭证总额不平衡，建议企业改正后重新采集；',1,10)
end

IF len(@fx) >0
begin
insert into xdata..facheck
values(@XID,'存在以下凭证的借贷发生额不平衡：'+@fx,1,11)
end

IF @yc <> 0
begin
 insert into xdata..facheck
 values(@XID,'凭证日期存在异常：存在日为0的凭证！',1,13)
end

IF len(@final) >0
begin
insert into xdata..facheck
values(@XID,'以下凭证对应科目为非末级科目：'+@final,3,14)
end
 
if len(@leakage)>0
begin
insert into xdata..facheck
values(@xid,'存在断号的凭证编号序列为：'+@leakage,1,12)
end
  
END
			
-- 辅助项目校验
if exists(select * from sys.objects where name = 'xmye')
BEGIN							
--4 空名称的辅助核算项目
declare @item table
(
kmdm nvarchar(1000),
xmdm nvarchar(1000)
)
declare @fz nvarchar(max)

insert into @item
select A1.Accountcode,A1.AuxiliaryCode from AuxiliaryFDetail A1 inner join xm x1 
on ltrim(rtrim(A1.AuxiliaryCode)) = ltrim(rtrim(x1.Xmdm)) collate Chinese_PRC_CS_AS_KS_WS 
where (x1.Xmmc is null or len(x1.Xmmc)<1)

if exists(select 1 from @item)
begin 
set @fz =(select fz +' ;' from(select '科目编码:'+ kmdm + ' ,辅助核算项目编码：'+ xmdm AS fz  from @item)A for xml path(''))

insert into xdata..facheck
values(@XID,'存在空名称的辅助核算项目：'+@fz,3,4)

end

--5 检验未定义辅助核算项目
declare @unproject table
(
kmdm nvarchar(1000),
xmdm nvarchar(1000)
)
declare @ft nvarchar(max)

insert into @unproject
select A1.Accountcode,a1.AuxiliaryCode  from AuxiliaryFDetail A1 where not exists
(select 1 from xm where ltrim(rtrim(A1.AuxiliaryCode)) = ltrim(rtrim(xm.Xmdm)) collate Chinese_PRC_CS_AS_KS_WS )

if exists(select 1 from @unproject)
begin
set @ft = (select b + ' ;' from(select '科目编码：' + kmdm + ' ,辅助项目编码：' + xmdm as b from @unproject)A for xml path(''))

insert into xdata..facheck
values(@XID,'存在以下辅助项目没有在财务系统中设置：'+ @ft ,3,5)
end

--7 校验期初余额一致性
declare @uniformity table
(
kmdm varchar(1000)
)
 create table  #KMXMTYPE 
(
KMDM	VARCHAR(1000),
KMMC VARCHAR(1000),
XMDM VARCHAR(1000),	
NCYE decimal(19,3),
TYPECODE	VARCHAR(1000)
 )
declare @balance varchar(1000)

 insert into #KMXMTYPE
 SELECT	DISTINCT x1.kmdm,
 (select kmmc from km where kmdm= x1.kmdm)as kmmc
 ,x1.XMDM,x1.Ncye,icl.FITEMID	FROM	
	xmye x1
	JOIN xm xm
	ON x1.Xmdm  =xm.Xmdm	
	INNER JOIN t_itemclass	icl
	ON LEFT(xm.Xmdm,LEN(icl.FItemId))=icl.FItemId

insert into @uniformity
select distinct t.kmdm from 
(select x1.KMDM,sum(x1.Ncye) as fzye,
(select Ncye from kmye where x1.Kmdm collate Chinese_PRC_CS_AS= Kmdm) as mxye
from #KMXMTYPE x1 
group by x1.KMDM,x1.TYPECODE)t where t.mxye - t.fzye <>0
intersect
select distinct t.kmdm from 
(select x1.KMDM,sum(x1.Ncye) as fzye,
(select Ncye from kmye where x1.Kmdm collate Chinese_PRC_CS_AS= Kmdm) as mxye
from #KMXMTYPE x1 
group by x1.KMDM)t where t.mxye - t.fzye <>0

if exists(select 1 from @uniformity)
begin
set @balance =(
select  '['+balance+']; ' from(select  '科目编码：'+u.kmdm+', 科目名称：'+k.Kmmc as balance 
from @uniformity u inner join km k on u.kmdm collate Chinese_PRC_CS_AS= k.Kmdm)A for xml path(''))

 insert into xdata..facheck
 values(@XID,'以下科目期初金额与其辅助项目汇总金额存在差异：'+@balance ,1,7)
end

 --9  项目余额唯一性校验
 declare @proRepeat nvarchar(max)
 if exists (select Kmdm,Xmdm from xmye x1 group by Kmdm,Xmdm having count(*) >1)
 begin
 set @proRepeat = (
 select  '['+pro+']; ' from (
 select '科目编码：'+ Kmdm + '; 科目名称：' + (select kmmc from km where x1.Kmdm = Kmdm) +'; 辅助项目编码：'+ Xmdm 
 +'; 辅助项目名称：' +(select xmmc from xm where x1.Xmdm = Xmdm) as pro
 from xmye x1 group by Kmdm,Xmdm having count(*) >1)A for xml path(''))

 insert into xdata..facheck
 values(@XID,'存在重复的项目余额：'+@proRepeat,1,9)
 end 

--4.6 已定义但未使用辅助核算（针对凭证表）
declare @notuse nvarchar(max) =''

set @notuse = (
select '['+nouse+']; ' from (
select '辅助核算类型编码：' + FItemClassID+ '；辅助核算类型名称：'+fname as nouse from t_itemclass where FItemId not in(
SELECT DISTINCT icl.FITEMID  FROM AuxiliaryFDetail _xmye  
INNER JOIN t_itemclass icl   
ON LEFT(ltrim(rtrim(_xmye.AuxiliaryCode)), LEN(ltrim(rtrim(icl.FItemId))))= ltrim(rtrim(icl.FItemId)) COLLATE Chinese_PRC_CS_AS_KS_WS
))A for xml path(''))

if len(@notuse)>0
begin
insert into xdata..facheck
values(@XID,'存在已定义但未使用的辅助核算项目; '+@notuse,1,15)
end
drop table #KMXMTYPE
END

if not exists(select * from xdata..facheck where XID = @XID)
insert into xdata..facheck
values(@XID,'财务数据校验通过，允许导入数据',1,100)

END TRY
begin catch          
 EXEC DBO.[PRO_THROW] @XID,'VerifyFinancialData'          
end catch          
        