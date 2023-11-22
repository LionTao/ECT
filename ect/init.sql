create table regions (id serial, name varchar, geom geometry(Polygon, 4326));
create index regions_geom_idx on regions using gist(geom);