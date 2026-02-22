my $sql1 = "insert into dwd.orders select * from ods.orders_src";
my $sql2 = q{create table ads.order_summary as select a.id from dwd.orders a join dim.shop s on a.shop_id=s.id};
