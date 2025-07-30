# =============================================================================
# Author    : Aastha Mohapatra
# Date      : 2025-06-16
# Purpose   : Gramodaya Mapping Plan - Cleaned Full Code (Setup-compatible)
# =============================================================================
# ------------------ 0. Source Setup ------------------
source(file.path(dirname(dirname(this.path::this.dir())), "global_setups", "0_setup_r.r"))

# ------------------ 1. Load Gramodaya Cleaned Data ------------------
gramo <- read.csv(file.path(paths$cleaned, "community_cleaned.csv")) %>%
  mutate(
    village_id = as.character(village_id),
    district_name = str_to_lower(str_trim(district_name)),
    block_name = str_to_lower(str_trim(block_name)),
    village_name = str_to_lower(str_trim(village_name))
  )

# ------------------ 2. Load Block Shapefile ------------------
block_shp <- st_read(file.path(paths$shapefile, "block_boundaries copy", "Odisha_Admin_Block_BND_2021.shp")) %>%
  mutate(
    block_name = str_to_lower(str_trim(block_name)),
    district_name = str_to_lower(str_trim(district_n))
  )

# ------------------ 3. Load Village Shapefile ------------------
village_shp <- st_read(file.path(paths$shapefile, "village_boundaries copy", "Odisha_Admin_Census_Village_BND_2021.shp")) %>%
  mutate(
    district_name = str_to_lower(str_trim(district_n)),
    block_name = str_to_lower(str_trim(block_name)),
    village_name = str_to_lower(str_trim(census_vil)),
    census_cod = as.character(census_cod)
  ) %>%
  as.data.table()

# ------------------ 4. Fuzzy Match: Gramodaya + Village Shapefile ------------------
gramo_clean <- gramo %>%
  mutate(village_name = str_trim(tolower(village_name)))

village_shp_clean <- village_shp %>%
  mutate(census_vil = str_trim(tolower(village_name)))

masala_sf <- stringdist_inner_join(
  gramo_clean,
  village_shp_clean,
  by = c("village_id" = "census_cod", "village_name" = "census_vil"),
  method = "jw",
  max_dist = 0.1,
  distance_col = "dist"
) %>%
  st_as_sf()

# ------------------ 5. Load District Outline ------------------
district_shp <- st_read(file.path(paths$shapefile, "district_boundaries copy", "Odisha_Admin_District_BND_2021.shp")) %>%
  mutate(district_name = str_to_lower(str_trim(district_n)))

sf_use_s2(FALSE)
odisha_outline <- st_union(district_shp) %>% st_as_sf()
sf_use_s2(TRUE)

# ------------------ 6. Clean and Filter Blocks ------------------
masala_sf <- masala_sf %>%
  mutate(block_name.y = str_to_lower(str_trim(block_name.y)))

block_shp <- block_shp %>%
  mutate(block_name = str_to_lower(str_trim(block_name)))

blocks_to_keep <- unique(masala_sf$block_name.y)
filtered_block_shp <- block_shp %>% filter(block_name %in% blocks_to_keep)

# Add additional LWE blocks
extra_blocks_to_add <- c("baliguda", "bandhugaon", "bhawanipatana", "boden", "boipariguda",
                         "chandahandi", "chandrapur", "chitrakonda", "dabugaon", "dharmagarh",
                         "golamunda", "gudari", "jharabandh", "jharigaon", "kalimela", "k.singpur",
                         "kantamala", "khairput", "khaprakhol", "komana", "kotagarh", "lamtaput",
                         "lanjigarh", "madanpur ramapur", "mathili", "muniguda", "nandapur",
                         "narayanapatna", "nuapada", "padmapur", "paikmal", "firingia", "podia",
                         "potangi", "raighar", "rayagada", "sinapali", "thuamul rampur", "tumudibandha")

extra_blocks <- block_shp %>% filter(block_name %in% extra_blocks_to_add)
filtered_block_shp <- bind_rows(filtered_block_shp, extra_blocks) %>%
  distinct(block_name, .keep_all = TRUE)

# ------------------ 7. Filter Districts ------------------
districts_to_keep <- unique(masala_sf$district_n)
filtered_district_shp <- district_shp %>%
  filter(district_n %in% districts_to_keep)

# ------------------ 8. Plot Map ------------------
ggplot() +
  geom_sf(data = masala_sf, aes(geometry = geometry), color = "blue", size = 0.6) +
  geom_sf(data = filtered_block_shp, fill = NA, color = "firebrick", linetype = "dotted", linewidth = 0.5) +
  geom_sf(data = filtered_district_shp, fill = NA, color = "black", linetype = "dotted", linewidth = 0.3) +
  geom_sf(data = odisha_outline, fill = NA, color = "darkgreen", linewidth = 0.2) +
  labs(
    title = "Strict-District Matched Gramodaya Villages and Blocks in Odisha",
    subtitle = "Blue = Villages | Red = Gramodaya Blocks | Black = Districts | Green = Odisha Outline",
    caption = "Source: PR&DW | June 2025"
  ) +
  theme_minimal()

# ------------------ 9. Save Final Matched Shapefile ------------------
st_write(
  masala_sf,
  dsn = file.path(paths$output, "matched_villages.shp"),
  delete_dsn = TRUE
)

# ------------------ END ------------------
