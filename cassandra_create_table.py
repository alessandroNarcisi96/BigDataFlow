from cassandra.cluster import Cluster
clstr=Cluster()
session=clstr.connect('mykeyspace')
qry= '''
create table pinterest_6 (
   unique_id text,
   category text,
   title text,
   description text,
   follower_count int,
   tag_list text,
   is_image_or_video text,
   image_src text,
   downloaded boolean,
   save_location text,
   primary key(unique_id));'''
session.execute(qry)
