
/***********
Project :  导入财务数据校验
errortype : 1-提示；2-警告；3-错误

***********/

CREATE Procedure VerifyFinancialData

@XID int --xifiles的xid字段
AS

begin try

SET NOCOUNT ON 


--1 财务数据可审性校验
--1.1 检验期初余额或本期凭证，至少有一项为非空
if not exists(select 1 from dbo.jzpz) AND not exists(select * from dbo.kmye)

BEGIN

insert into xdata..facheck
values(@XID,'企业财务数据没有期初金额与本期凭证，请核实原因并经企业改正后重新采集；',3)

END


--2 科目/项目完整性校验
--2.1 检查是否存在空名称的会计科目（找kmdm）
declare @subject table(ID int IDENTITY(1,1),kmdm varchar(255))
declare @number nvarchar(max)=''
declare @i int 

insert into @subject
select k1.kmdm from kmye k1 inner join km k2 on k1.Kmdm = k2.Kmdm where (k2.Kmmc is null or len(k2.Kmmc)<1)
union 
select j1.Kmdm from jzpz j1 inner join km k2 on j1.Kmdm = k2.Kmdm where (k2.Kmmc is null or len(k2.Kmmc)<1)

if exists(select 1 from @subject) begin
set @number =( select kmdm+' ;' from  @subject for xml path(''))

insert into xdata..facheck
values(@XID,'存在以下会计科目编号的名称为空:'+@number +'请核实原因并经企业改正后重新采集；',3)
end

--2.2 检查期初余额表是否使用未定义会计科目(两种情况：a是将余额表与km表对比，是否有不存在km表里的科目;b是与财政部规定的一级会计科目比较)
declare @undefined table
(
 id int identity(1,1),
 kmdm  varchar(255) ,
 kmmc varchar(255),
 kmjb varchar(255)
)
set @number=''
set @i = 0

insert into @undefined(kmdm,kmmc,kmjb)
select  k1.Kmdm,k2.Kmmc,k2.Kmjb from kmye k1
inner join km k2 on k1.Kmdm = k2.Kmdm
where k2.Kmjb = 1 and not exists(select 1 from EASKMDZB where k2.Kmmc COLLATE Chinese_PRC_CI_AS= Note) and not exists(select 1 from TBFS where k2.kmmc COLLATE Chinese_PRC_CI_AS= FsName) 
union
select k1.Kmdm,'','' from kmye k1 where not exists(select 1 from km  where k1.Kmdm = km.Kmdm) 

if exists (select 1 from @undefined)
begin 
set @number =( select  '['+ info +']; ' from 
(select '科目编码：'+ kmdm+' 科目名称：' + isnull(kmmc,'') as info from @undefined)A for xml path(''))

insert into  xdata..facheck
values(@XID,'期初余额存在以下未定义的会计科目:'+ @number+'请核实原因并经企业改正后重新采集；',3)
end 


--2.2 检查凭证表是否使用未定义会计科目(两种情况：a是将凭证表与km表比较，查出不存在km表里的科目；b是匹配出每笔凭证的一级科目并与财政部规定的一级科目比较)
delete from @undefined
set @number=''
set @i = 0

insert into @undefined(kmdm,kmmc,kmjb)
select j1.dm,k1.Kmmc,k1.Kmjb from 
(select distinct left(ltrim(Kmdm),5) as dm from jzpz) j1
inner join km k1 on j1.dm = k1.Kmdm
where k1.kmjb = 1 and not exists (select 1 from EASKMDZB where k1.Kmmc COLLATE Chinese_PRC_CI_AS= Note) and  not exists (select 1 from TBFS where k1.Kmmc COLLATE Chinese_PRC_CI_AS= FsName)
union
select j1.Kmdm,'','' from jzpz j1 where not exists(select 1 from km where j1.Kmdm = km.Kmdm)

if exists (select 1 from @undefined)
begin
set @number = (select  '['+ info +']; ' from 
(select '科目编码：'+ kmdm+' 科目名称：' + isnull(kmmc,'') as info from @undefined)A for xml path('')
)

insert into  xdata..facheck
values(@XID,'凭证中存在以下未定义的会计科目:'+ @number+'请核实原因并经企业改正后重新采集；',3)

end 
							

--2.3 检验存在空名称的辅助核算项目
--期初余额表
--select * from sys.all_columns where OBJECT_ID in(
--select OBJECT_ID from sys.objects where name ='t_itemdetail')
--select x1.Kmdm,x1.Xmdm from xmye x1 inner join xm x2 on x1.Kmdm = x2.Xmdm where (x2.Xmmc is null or len(x2.Xmmc)<1 )

--2.4 检验余额表、凭证表中使用未定义辅助核算项目

--3 期初余额校验
--3.1 检验期初余额是否平衡
declare @ncye decimal(19, 3)
select @ncye=sum(ncye) from kmye where  ismx=1
if @ncye <> 0
begin
insert into  xdata..facheck
values(@XID,'企业财务数据期初余额不平,请核实原因并经企业改正后重新采集；',3)
end

--3.2 校验期初余额一致性
declare @uniformity table
(
kmdm varchar(1000),
je decimal(19,3)
)
 create table  #KMXMTYPE 
(
KMDM	VARCHAR(1000),
XMDM VARCHAR(1000),	
NCYE decimal(19,3),
TYPECODE	VARCHAR(1000)
 )
declare @je decimal(19,3)
declare @balance varchar(1000)

 insert into #KMXMTYPE
 SELECT	DISTINCT x1.KMDM,x1.XMDM,x1.Ncye,icl.FITEMID	FROM	
	xmye x1
	JOIN xm xm
	ON x1.Xmdm  =xm.Xmdm	
	INNER JOIN t_itemclass	icl
	ON LEFT(xm.Xmdm,LEN(icl.FItemId))=icl.FItemId

insert into @uniformity(kmdm,je)
select t.kmdm,t.mxye - t.fzye from  
 (select x1.Kmdm,sum(x1.Ncye) as fzye,
 (select Ncye from kmye where x1.Kmdm = Kmdm) as mxye
 from #KMXMTYPE x1 
 group by x1.KMDM,x1.TYPECODE ) t order by t.KMDM


if exists(select 1 from @uniformity where je <> 0)
 begin
 set @balance= (select '['+a+']; ' from 
  (select '科目编码：'+kmdm+', 该明细科目与辅助汇总金额存在差额：'+cast([je] as varchar) as a  from @uniformity where je <>0)A for xml path(''))

 insert into xdata..facheck
 values(@XID,'存在明细科目期初金额与辅助汇总金额不一致：'+@balance ,1)
 end

--4 本期发生额校验
--4.1 总额平衡校验
declare @total decimal(19,3)

select @total= sum(rmb)-(select sum(rmb) from jzpz where jd = '贷') from jzpz where Jd = '借'
if @total <> 0
begin

 insert into xdata..facheck
 values(@XID,'企业财务数据本期凭证发生额不平衡，建议企业改正后重新采集；',1)
end 

--4.2 分项平衡校验(检查每一笔凭证借贷发生额是否平衡)
declare @subitem table(
ID int identity(1,1),
pzbh_date nvarchar(1000),
jd nvarchar(10),
rmb decimal(19,3)
)
declare @fx varchar(max)

insert into @subitem(pzbh_date,jd,rmb)
select Pzh+','+pz_date,Jd,case when Jd = '借' then Rmb else (-1)*Rmb end
 from jzpz

if exists(select 1 from @subitem group by pzbh_date having sum(rmb) <> 0)begin 
 set @fx = (select '['+a+']; ' from 
(select '凭证编号：'+stuff(pzbh_date,CHARINDEX(',',pzbh_date)+1,0,' 凭证日期：') AS a from @subitem  group by pzbh_date having sum(rmb) <> 0)
A for xml path(''))

insert into xdata..facheck
values(@XID,'存在以下凭证的借贷发生额不平衡：'+@fx+'建议企业改正后重新采集;',1)
 end

--4.3 凭证编号连续性校验
declare @succession table 
 (
 pzbh int ,
 months varchar(10)
 )
create table #LSTAB
 (
 months varchar(10),
 goid varchar(10)
 )
 declare @maxid int,@goint int,@maxmonth  int,@minmonth int
 declare @leakage nvarchar(max)

 insert into @succession
 select distinct Pzbh,SUBSTRING(ltrim(Pzrq),1,1) from jzpz  

 select @minmonth = min(months),@maxmonth = max(months) from @succession

 while @minmonth <= @maxmonth 
 begin
 select @maxid = max(pzbh) from @succession where months = @minmonth
 set @goint = 0

 WHILE @goint <@maxid BEGIN
 SET @goint = @goint + 1
 INSERT INTO #LSTAB(months,goid) VALUES(@minmonth,@goint)
 END

 set @minmonth = @minmonth + 1
 end

 delete from #LSTAB where  EXISTS (SELECT 1 FROM @succession  WHERE #LSTAB.goid = pzbh AND #LSTAB.months = months)

 if exists(select * from #LSTAB )
 begin
  set @leakage= (select  '['+ bh +'];' from (
 select months+'月份存在缺失的凭证号：'+stuff(numList,len(numList),1,NULL) as bh FROM
 (select L.months,(
  select goid+',' from #LSTAB
  where  months = L.months 
  FOR XML PATH('')) AS numList
  from #LSTAB L 
  group by months) A)B FOR XML PATH(''))

  insert into xdata..facheck
  values(@XID,@leakage,1)
 end

--4.4 凭证日期异常
 if exists( select *  from  jzpz where right(rtrim(pz_date),2)=0)
 begin
 insert into xdata..facheck
 values(@XID,'凭证日期存在异常：存在日为0的凭证，已自动更正！',1)

 update jzpz set pz_date = stuff(ltrim(pz_date),9,2,'01') where right(rtrim(pz_date),2)=0
end

--4.5 使用非末级科目（1.有辅助核算项目的科目，没有使用辅助核算项目；2.无辅助核算的科目，没有使用最末级明细科目）
--select * from jzpz where FDetailID <> 0 or FDetailID is not null

--select * from km

--4.6 已定义但未使用辅助核算（针对凭证表）
--select * from km where Kmmx <>'1.000'


drop table #KMXMTYPE
drop table #LSTAB

END TRY
begin catch          
 EXEC DBO.[PRO_THROW] @XID,'VerifyFinancialData'          
end catch          
        