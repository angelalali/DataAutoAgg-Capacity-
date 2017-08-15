import pyodbc
import pandas as pd

cnxn = pyodbc.connect(
        driver='{SQL Server}',
        host='WDPRDRPTDB',
        database='SpacexERP',
        Trusted_Connection='yes',
    )

def qpv_data ():
    query = """
        select distinct
        --ModelName,
        DetailPartNumber as PartNumber
        , PartDescription as QPVPartDescription,
        PartID,
        PartSystem,
        SupplierCode,
        SupplierName as QPVSupplierName,
        Buyer,
        Manager,
        POType,
        PurchaseUnitID,
        StockUnitID,
        UnitPrice,
        ExchangeRate,
        PartQuantityPerVehicle,
        UnitPrice * ExchangeRate * PartQuantityPerVehicle as CPV
        , FabricatedFlag
        , coalesce(PurchasedFlag, SubContractFlag) NonFabricated
        --, TagID
        --, TagName
        --, v.Variant

        from ( -- eve; everything

            ------------------------- this file pulls the PO Unit Price from each supplier ------------------
            SELECT
            usage.Name as ModelName,
            rpn.DetailPartNumber,
            rpn.PartDescription,
            --rpn.PurchaseOrderTypeID,
            --sp.SAPPONumber,
            rpn.PartID,
            rpn.PartSystem,
            --rpn.SupplierCode,
            --rpn.SupplierName,

            june.PurchaseOrderTypeID as POType,
            coalesce(june.SupplierCode, rpn.preferredsuppliercode) as SupplierCode,
            coalesce(june.SupplierName, rpn.preferredsuppliername) as SupplierName,
            june.CurrencyCode as CurrencyCode,
            --june.CurrencyName as JuneCurrencyName,
            coalesce(june.Exchangerate,1) as ExchangeRate,
            june.PurchaseUnitID as PurchaseUnitID,
            june.StockUnitID as StockUnitID,
            Coalesce(usage.Quantity/
                    CASE
                        WHEN june.StockUnitID=june.PurchaseUnitID THEN 1
                        WHEN june.PurchaseUnitID IS NULL THEN 1
                        ELSE 1/COALESCE(junuc.ConversionFactor,1/junuc2.ConversionFactor,junpuc.ConversionFactor,1/junpuc2.ConversionFactor) END,0)
                        as PartQuantityPerVehicle,

            june.UnitPrice as UnitPrice

            , rpn.FabricatedFlag
            , rpn.PurchasedFlag
            , rpn.SubContractFlag
            , bmd.Buyer
            , bmd.Manager
            --, rpn.TagID
            --, rpn.TagName

            --select *
            ------------------- use following to reduce the size of your table ---------------
            from
            (
                ---------------------------------- select partnumber belong to model 3 -----------------------
                select
                distinct DetailPartNumber,
                --po.purchaseorderTypeID,
                --SalesBOMSampleID,
                --BOMExplosionDate,
                --BOMPartNumber as TopLevelNumber,
                --max(RunningStartDate) as RunnintStartDate,
                --RunningEndDate,
                p.Description as PartDescription,
                p.PartID,
                p.PartSystem,
                --p.StockUnitID,
                --pol.PurchaseUnitID,
                p.BuyerCode
                , p.FabricatedFlag
                , p.PurchasedFlag
                , p.SubContractFlag
                , s2.SupplierCode as PreferredSupplierCode
                , s2.Name as PreferredSupplierName
                --, t.TagID
                --, t.TagName

                from bom.SalesBOMSampleExplosion be
                left join inv.Part p
                on be.DetailPartNumber = p.PartNumber
                left join pur.PurchaseOrderLine pol
                on p.PartID = pol.PartID
                left join pur.PurchaseOrder po
                on pol.PurchaseOrderID = po.PurchaseOrderID

                --left join ap.Supplier s1
                --on s1.SupplierID = po.SupplierID
                left join ap.Supplier s2
                on s2.SupplierID = p.PreferredSupplierID

                -- find tag (to be used with cpv info)
                left join inv.PartTag pt
                on pt.PartID = p.PartID
                --left join inv.Tag t
                --on pt.TagID = t.TagID

                where
                --SalesBOMSampleID in (149, 150) and
                (GETDATE() - BOMExplosionDate) < 7
            ) rpn -- required PN



            -------------------------------- grabbing the current price ------------------------------------
            left join
            ( -- june
                select *
                    from (
                    SELECT
                    PartNumber,
                    --SAPPONumber,
                    --PartDescription,
                    PurchaseOrderTypeID,
                    SupplierCode,
                    SupplierName,
                    UnitPrice,
                    CurrencyCode,
                    CurrencyName,
                    ExchangeRate,
                    PurchaseUnitID,
                    StockUnitID,
                    PartID,
                    --BuyerCode,
                    ROW_NUMBER() OVER (PARTITION BY PartNumber ORDER BY POCreateDate DESC, POLCreateDate desc) AS 'rank'
                    from ( -- complete
                        --------------------------- Getting PO Line w/o date based pricing --------------------------
                        Select price.*
                        from ( --price
                            select
                            --po.SAPPONumber,
                            po.PurchaseOrderTypeID,
                            pol.PurchaseOrderLineID,
                            p.PartNumber,
                            --p.Description as PartDescription,
                            pol.UnitPrice,
                            c.CurrencyCode,
                            c.Name as CurrencyName,
                            c.ExchangeRate,
                            pol.CreateDate as POLCreateDate,
                            po.CreateDate as POCreateDate,
                            pol.PurchaseUnitID,
                            p.StockUnitID,
                            p.PartID,
                            --p.BuyerCode,
                            s.SupplierCode,
                            s.Name as SupplierName
                            --coalesce(s.SupplierCode, s2.SupplierCode) as SupplierCode,
                            --coalesce(s.Name, s2.Name) as SupplierName
                            from pur.PurchaseOrder as po with (NOLOCK)
                            left join pur.PurchaseOrderLine as pol on po.PurchaseOrderID=pol.PurchaseOrderID
                            left join inv.Part as p on pol.PartID=p.PartID
                            left join ap.Supplier as s on po.SupplierID=s.SupplierID
                            --left join ap.Supplier as s2 on p.PreferredSupplierID = s.SupplierID
                            left join gl.Currency c on po.CurrencyID = c.CurrencyID

                            where
                                po.Status in ('C','R')
                                -- R = released; C = closed
                                and (pol.DeliveryCompleteFlag IS NULL or pol.DeliveryCompleteFlag=0)
                                and (pol.ReturnFlag IS NULL or pol.ReturnFlag=0)
                                and pol.LineStatus in ('A', 'C')  -- A = active; open line; C = closed
                                and po.PurchaseOrderTypeID in (1,8, 9,17)
                                and pol.UnitPrice <> 0
                        ) as price

                        left join ( --alldps
                        --- exclude the ones w date base pricing ---
                            select distinct pol.PartID
                            from pur.PartSupplierDatePricing pdp
                            INNER JOIN inv.part pa ON pdp.PartID=pa.PartID
                            INNER JOIN pur.PurchaseOrderLine pol ON pa.PartID=pol.PartID
                            INNER JOIN pur.PurchaseOrder po ON po.PurchaseOrderID=pol.PurchaseOrderID
                            --INNER JOIN ap.Supplier su ON su.SupplierID=pdp.SupplierID AND su.SupplierID=po.SupplierID
                            WHERE
                                po.Status='R' AND
                                pol.linestatus='A' AND
                                ( pol.DeliveryCompleteFlag IS NULL OR pol.DeliveryCompleteFlag=0) AND
                                pdp.StartDate <= '2017-06-30'
                                and (pol.ReturnFlag IS NULL or pol.ReturnFlag=0)
                                and po.PurchaseOrderTypeID=9

                        ) as alldps on price.PartID=alldps.PartID
                        where alldps.PartID is null
                        --) as alldps on price.PurchaseOrderLineID=alldps.PurchaseOrderLineID
                        --where alldps.PurchaseOrderLineID is null
                        --and price.PartNumber in ('1109531-00-A')


                        UNION ALL

                        --------------------------- trying to grab all date based price data ---------------------------------
                        Select *
                        from (  --test2
                            SELECT
                            --SAPPONumber,
                            PurchaseOrderTypeID,
                            PurchaseOrderLineID,
                            PartNumber,
                            --PartDescription,
                            UnitPrice,
                            CurrencyCode,
                            CurrencyName,
                            ExchangeRate,
                            POLCreateDate,
                            POCreateDate,
                            PurchaseUnitID,
                            StockUnitID,
                            PartID,
                            --BuyerCode,
                            SupplierCode,
                            SupplierName
                            from ( -- dpr
                                SELECT
                                --dpr.SAPPONumber,
                                dpr.PurchaseOrderTypeID,
                                dpr.PurchaseOrderLineID,
                                dpr.SupplierCode,
                                dpr.SupplierName,
                                --dpr.BuyerCode,
                                --PartDescription,
                                dpr.PartNumber,
                                dpr.POLCreateDate,
                                dpr.POCreateDate,
                                dpr.PurchaseUnitID,
                                dpr.StockUnitID,
                                dpr.PartID,
                                dpr.CurrencyCode,
                                dpr.CurrencyName,
                                dpr.ExchangeRate,
                                CASE
                                    WHEN dpr.cf1 IS NULL AND dpr.cf2 IS NULL THEN dpr.UnitPrice*dpr.ExchangeRate
                                    WHEN dpr.cf1 IS NOT NULL THEN dpr.UnitPrice*dpr.ExchangeRate/dpr.cf1
                                    WHEN dpr.cf2 IS NOT NULL THEN dpr.UnitPrice*dpr.ExchangeRate*dpr.cf2
                                END AS 'UnitPrice',
                                ROW_NUMBER() over (partition BY dpr.PurchaseOrderLineID order by dpr.StartDate DESC) as 'rank'
                                FROM ( --er
                                    SELECT
                                    --po.SAPPONumber,
                                    po.PurchaseOrderTypeID,
                                    pol.SAPPOLineNo,
                                    pol.PurchaseOrderLineID,
                                    pol.PurchaseUnitID,
                                    pol.CreateDate as POLCreateDate,
                                    po.CreateDate as POCreateDate,
                                    pa.PartNumber,
                                    pa.StockUnitID,
                                    pa.PartID,
                                    --pa.BuyerCode,
                                    --pa.Description as PartDescription,
                                    pdp.UnitPrice,
                                    pdp.CurrencyID 'PricingCurrencyID',
                                    pocu.CurrencyID 'POCurrencyID',
                                    paun.UnitID 'PartUnitID',
                                    poun.UnitID 'LineUnitID',
                                    pdp.StartDate,
                                    pocu.CurrencyCode,
                                    pocu.Name as CurrencyName,
                                    CASE
                                        WHEN pdp.CurrencyID=pocu.CurrencyID THEN 1 ELSE COALESCE(er.ExchangeRate,1)
                                    END AS 'ExchangeRate',
                                    uc1.ConversionFactor 'CF1',
                                    uc2.ConversionFactor 'CF2',
                                    su.SupplierCode,
                                    su.Name as SupplierName
                                    --coalesce(su.SupplierCode, su2.SupplierCode) as SupplierCode,
                                    --coalesce(su.Name, su2.Name) as SupplierName
                                    FROM pur.PartSupplierDatePricing pdp
                                    INNER JOIN inv.part pa ON pdp.PartID=pa.PartID
                                    INNER JOIN pur.PurchaseOrderLine pol ON pa.PartID=pol.PartID
                                    INNER JOIN pur.PurchaseOrder po ON po.PurchaseOrderID=pol.PurchaseOrderID
                                    INNER JOIN ap.Supplier su ON su.SupplierID=pdp.SupplierID AND su.SupplierID=po.SupplierID
                                    --join ap.Supplier su2 on su.SupplierID = pa.PreferredSupplierID
                                    INNER JOIN inv.Unit paun ON paun.UnitID=pa.StockUnitID
                                    INNER JOIN gl.Currency pocu ON pocu.CurrencyID=po.CurrencyID
                                    INNER JOIN inv.Unit poun ON poun.UnitID = pol.PurchaseUnitID
                                    LEFT JOIN [SpacexERP].inv.PartUnitConversion uc1 (NOLOCK) ON uc1.FromUnitID=paun.UnitID AND uc1.ToUnitID=poun.UnitID AND uc1.PartID=pa.PartID
                                    LEFT JOIN [SpacexERP].inv.PartUnitConversion uc2 (NOLOCK) ON uc2.ToUnitID=paun.UnitID AND uc2.FromUnitID=poun.UnitID AND uc2.PartID=pa.PartID
                                    LEFT JOIN (
                                        SELECT *
                                        FROM [SpacexERP].gl.ExchangeRateMap (NOLOCK)
                                        WHERE
                                                exchangeratetype='Month-end' AND
                                                validfrom= DATEADD(mm, datediff(month, 0, getdate()), 0)-1
                                    )er ON er.FromCurrencyID=pdp.CurrencyID AND er.ToCurrencyID=po.CurrencyID
                                    WHERE
                                        po.Status='R' AND
                                        pol.linestatus='A' AND
                                        ( pol.DeliveryCompleteFlag IS NULL OR pol.DeliveryCompleteFlag=0) AND
                                        pdp.StartDate < '2017-06-30'
                                        and (pol.ReturnFlag IS NULL or pol.ReturnFlag=0)
                                        and po.PurchaseOrderTypeID=9
                                        and pol.UnitPrice <> 0
                                ) dpr
                            ) as mr
                            where rank=1

                        ) test2 -- end of 2nd union portion's from

                    ) as complete   -- end of 2nd from
                ) tmp
                where rank = 1
            ) as june  -- end of june month from
            on rpn.DetailPartNumber = june.PartNumber

            left join (
                Select
                --ss.SalesBOMSampleID, e.BOMExplosionDate,
                /*
                below is a conversion used to convert BOMexplosiondate into standard format (in case it's not exploded on Monday
                b/c before, we'd use this date to join w the pph table by using it's Week, which is always the 1st day of the week
                */
                --dateadd(wk, datediff(wk, 0, e.BOMExplosionDate), 0) as BOMExplosionDate,
                ss.Name, DetailPartNumber, UsageType, sum(runningquantity) as Quantity
                from (
                    select * from bom.SalesBOMSample with (NOLOCK) where CreateDate>='2015-10-10'
                ) ss
                JOIN (
                    select * from bom.SalesBOMSampleExplosion  with (NOLOCK)
                    where BOMExplosionDate = (
                        select Max(BOMExplosionDate) as BOMExplosionDate from bom.SalesBOMSampleExplosion with (NOLOCK)
                        )
                    ) e
                ON ss.SalesBOMSampleID = e.SalesBOMSampleID
                where
                --e.SalesBOMSampleID in ('149', '150') and
                --ss.Name like 'Model 3 - %' and
                --usagetype in ('Purchased', 'Sub-contract') and
                datediff(day, BOMExplosionDate, GETDATE()) < 7
                group by ss.SalesBOMSampleID, e.BOMExplosionDate,
                --(dateadd(wk, datediff(wk, 0, BOMExplosionDate), 0)),
                ss.Name, DetailPartNumber, UsageType
                --having DetailPartNumber like '1012041-00-C'
                --order by DetailPartNumber
            ) as usage
            --left join bom.SalesBOMSampleExplosion sbe
            on usage.DetailPartNumber = rpn.DetailPartNumber


            -- Unit conversion for June price

            LEFT JOIN inv.UnitConversion junuc (NOLOCK)  ON june.StockUnitID=junuc.FromUnitID AND june.PurchaseUnitID =junuc.ToUnitID
            LEFT JOIN inv.UnitConversion junuc2 (NOLOCK)  ON june.StockUnitID=junuc2.ToUnitID AND june.PurchaseUnitID=junuc2.FromUnitID
            LEFT JOIN inv.PartUnitConversion junpuc (NOLOCK)  ON june.PartID=junpuc.PartID AND june.StockUnitID=junpuc.FromUnitID AND june.PurchaseUnitID=junpuc.ToUnitID
            LEFT JOIN inv.PartUnitConversion junpuc2 (NOLOCK)  ON june.PartID=junpuc2.PartID AND june.StockUnitID=junpuc2.ToUnitID AND june.PurchaseUnitID=junpuc2.FromUnitID


            ----- get buyer manager info ------
            left join [sjc04-woppprd1].[supplychain_sustainability].pur.BuyerManagerDirector as bmd on rpn.BuyerCode=bmd.BuyerCode
        ) eve


    """

    ### pandas has a really convenient in house fxn that will just read the sql results into a dtaframe for you! :D
    return pd.read_sql(query, cnxn)

# returned columns include:
# ModelName	DetailPartNumber	PartDescription	PartID	PartSystem	JuneSupplierCode	JuneSupplierName	Buyer
# Manager	POType	JunePurchaseUnitID	JuneStockUnitID	JuneUnitPrice	JuneExchangeRate	JunePartQuantityPerVehicle
# JuneCPV	FabricatedFlag	NonFabricated	TagID	TagName
