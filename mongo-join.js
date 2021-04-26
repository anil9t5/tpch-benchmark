dbSource = db.getSiblingDB("tpch_scale_smallest");
dbDest = db.getSiblingDB("jointpch_smallest");
dbDest.deals.drop();

var date = new Date();


dbSource.lineitem.find().batchSize(1000).forEach(
    function (lineitem) {
        {
            // replace order
            order = dbSource.orders.findOne({ "O_ORDERKEY": lineitem.L_ORDERKEY });

            if (order != null)
                lineitem.order = order;

            delete (lineitem.L_ORDERKEY);
            {
                // replace customer

                customer = dbSource.customer.findOne({ "C_CUSTKEY": order.O_CUSTKEY });

                lineitem.order.customer = customer;
                delete (lineitem.order.O_CUSTKEY);
                delete (lineitem.order._id);
                {
                    // replace nation
                    nation = dbSource.nation.findOne({ "N_NATIONKEY": customer.C_NATIONKEY });

                    lineitem.order.customer.nation = nation;
                    delete (lineitem.order.customer.C_NATIONKEY);
                    delete (lineitem.order.customer._id);
                    {
                        // replace region

                        region = dbSource.region.findOne({ "R_REGIONKEY": nation.N_REGIONKEY });

                        if (region == "object") {
                            lineitem.order.customer.nation.region = region;
                            delete (lineitem.order.customer.nation.N_REGIONKEY);
                            delete (lineitem.order.customer.nation._id);
                            delete (lineitem.order.customer.nation.region._id);
                        }
                    }
                }
            }
        }
        {
            // replace partsupp
            partsupp = dbSource.partsupp.findOne(
                { "PS_PARTKEY": lineitem.L_PARTKEY, "PS_SUPPKEY": lineitem.L_SUPPKEY });

            lineitem.partsupp = partsupp;
            delete (lineitem.L_PARTKEY);
            delete (lineitem.L_SUPPKEY);
            delete (lineitem.partsupp._id);
            {
                // replace part
                part = dbSource.part.findOne({ "P_PARTKEY": partsupp.PS_PARTKEY });

                lineitem.partsupp.part = part;
                delete (lineitem.partsupp.PS_PARTKEY);
                delete (lineitem.partsupp.part._id);
            }
            {
                // replace supplier
                supplier = dbSource.supplier.findOne({ "S_SUPPKEY": partsupp.PS_SUPPKEY });

                lineitem.partsupp.supplier = supplier;
                delete (lineitem.partsupp.PS_SUPPKEY);
                delete (lineitem.partsupp.supplier._id);
                {
                    // replace nation
                    nation = dbSource.nation.findOne({ "N_NATIONKEY": supplier.S_NATIONKEY });

                    lineitem.partsupp.supplier.nation = nation;
                    delete (lineitem.partsupp.supplier.S_NATIONKEY);
                    delete (lineitem.partsupp.supplier._id);
                    {
                        // replace region

                        region = dbSource.region.findOne(
                            { "R_REGIONKEY": nation.N_REGIONKEY });
                        if (typeof (region) == "object") {

                            lineitem.partsupp.supplier.nation.region = region;
                            delete (lineitem.partsupp.supplier.nation.N_REGIONKEY);
                            delete (lineitem.partsupp.supplier.nation._id);
                            delete (lineitem.partsupp.supplier.nation.region._id);
                        }

                    }
                }
            }
        }
        var start_time = date.getTime();
        dbDest.deals.insert(lineitem);
        var end_time = date.getTime();
        print("In seconds: " + (end_time - start_time) / 1000);
    }
);
