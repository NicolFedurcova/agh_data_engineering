#!/usr/bin/env python
# coding: utf-8

# In[17]:

import geopandas as gpd
import json
import osmnx as ox
import contextily as cx
import matplotlib.pyplot as plt


# In[2]:


with open('proj4_params.json', encoding="utf-8") as json_file:
    params = json.load(json_file)
CRS = "EPSG:2178"
#CRS = "EPSG:3857"



# In[3]:


#TASK 1
points = gpd.read_file("proj4_points.geojson")

points = points.to_crs(CRS) #crs for poland representing positions in meters
points_copy = points.copy()




# In[4]:




points["buffer_geom"] = points.geometry.buffer(100) #object of 100 m radius around each lamp
name_of_id_column = params["id_column"]

df_joined = gpd.sjoin(
    points[[name_of_id_column, "geometry"]],
    points.set_geometry("buffer_geom"),
    predicate='within')

# In[5]:


#part 1
df_counted_grouped = df_joined.groupby(name_of_id_column+"_left")[name_of_id_column+"_right"].count()
df_counts = df_counted_grouped.reset_index()
df_counts.columns = [name_of_id_column, "count"]
df_counts.to_csv("proj4_ex01_counts.csv",index=False)

#part 2
points_with_geometry = points.merge(df_counts, on=name_of_id_column, how="left")
points_latlon = points_with_geometry.to_crs("EPSG:4326") #converting to geographic CRS - to get lat and lon
points_latlon["lon"] = points_latlon.geometry.x.round(7)
points_latlon["lat"] = points_latlon.geometry.y.round(7)
points_ready_for_csv = points_latlon.loc[:,[name_of_id_column, "lat","lon"]]
points_ready_for_csv.to_csv("proj4_ex01_coords.csv", index=False)



# In[6]:


#TASK 2
city = params["city"]
osm_tags = {
    'highway': [
        'tertiary'
    ]
}

gdf_city = ox.features_from_place(city, tags=osm_tags)
gdf_city = gdf_city.reset_index() #to acess multiindex
gdf_city = gdf_city.loc[:,["id","name","geometry"]]
gdf_city.columns=("osm_id","name","geometry")
gdf_city.to_file('proj4_ex02_roads.geojson', index=False)


# In[7]:


#TASK 3
gdf_city = gdf_city.to_crs(CRS)



# In[8]:


# gdf_city['buffer'] = gdf_city.geometry.buffer(50, cap_style=2)
# gdf_buffered = gpd.GeoDataFrame(gdf_city[['name']], geometry=gdf_city['buffer'], crs=gdf_city.crs)
# joined = gpd.sjoin(points_copy, gdf_buffered, predicate = "within")
# joined

#BETTER
gdf_city["buffer_street"] = gdf_city.geometry.buffer(50, cap_style=2)
gdf_city_buffered = gpd.GeoDataFrame(gdf_city[['name']], geometry=gdf_city['buffer_street'], crs=gdf_city.crs)
df_joined_street_point2 = gpd.sjoin(
    points_copy,
    gdf_city_buffered,
    predicate = "within"
)


# points_copy["buffer_street"] = points_copy.geometry.buffer(50,cap_style=1)
# df_joined_street_point2 = gpd.sjoin(
#     gdf_city[['name', 'geometry']],
#     points_copy.set_geometry('buffer_street'),
#     predicate = "within"
# )
# df_joined_street_point2


# In[9]:


# street_counts = joined.groupby('name')[name_of_id_column].count()
# street_counts

df_street_counted_grouped = df_joined_street_point2.groupby("name")[name_of_id_column].count()
df_counts = df_street_counted_grouped.reset_index()
df_counts.columns = ["name", "point_count"]
df_counts = df_counts[df_counts["point_count"] > 0]
df_counts.to_csv("proj4_ex03_streets_points.csv",index=False)



# In[21]:


#TASK 4
gdf_countries = gpd.read_file("proj4_countries.geojson")
gdf_countries.to_crs(epsg=3857, inplace=True)
gdf_countries.to_pickle("proj4_ex04_gdf.pkl")



# In[29]:


# for i, row in gdf_countries.iterrows():
#     country_name = row["name"]
#     country_geometry = gpd.GeoDataFrame([row], crs=gdf_countries.crs)
    
#     #facecolor none removes fill
#     #edgecolor=blue outlines shapes
#     ax = country_geometry.plot(edgecolor='blue', facecolor='none', linewidth=2)

#     cx.add_basemap(ax, crs=gdf_countries.crs.to_string())

#     fig = ax.get_figure()
#     filename = f"proj4_ex04_{country_name.lower().replace(' ', '_')}.png"
#     fig.savefig(filename)
#     plt.close(fig)

