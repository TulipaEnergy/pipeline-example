### A Pluto.jl notebook ###
# v0.19.42

using Markdown
using InteractiveUtils

# ╔═╡ 1d505b9e-31fd-11ef-1f7a-2171b11023ea
begin
	# This is ensuring that we use the Project.toml information. For a generally useful notebook, we should use the normal Pluto strategy (in the first cell).
	import Pkg
	Pkg.activate(Base.current_project())
	Pkg.instantiate()
	using CSV: CSV
	using DataFrames: DataFrames as DF, DataFrame
	using DuckDB: DuckDB as DDB, DBInterface
	using PlutoUI: PlutoUI as PUI
	using TulipaClustering: TulipaClustering as TC
	using TulipaEnergyModel: TulipaEnergyModel as TEM
	using TulipaIO: TulipaIO as TIO
	using SparseArrays: findnz, sparse
end

# ╔═╡ 6876e975-7525-4139-803b-7bc7d939d6b5
# using CSV, DataFrames, DuckDB, PlutoUI , TulipaClustering , TulipaEnergyModel , TulipaIO, SparseArrays

# ╔═╡ 5e8526b5-f13c-45a5-9d61-8acdfa014f50
# begin
# 	DF = DataFrames
# 	DDB = DuckDB
# 	PUI = PlutoUI
# 	TC = TulipaClustering
# 	TEM = TulipaEnergyModel
# 	TIO = TulipaIO
# end

# ╔═╡ a46312ec-7cc7-432b-8965-a82f4a163a4b
PUI.TableOfContents(depth=4)

# ╔═╡ 75094888-27c7-430c-8a77-38d104b5b389
md"""
# Pipeline example

This pipeline will show how to use the Tulipa ecosystem to read data, cluster the profiles, create and solve the energy model problem, and plot the result.

We should aim to use `TulipaIO/DuckDB` whenever we need to manipulate data in this file.

!!! danger "WIP"
	This is a work in progress.
"""

# ╔═╡ e4688e71-118a-40e6-a060-18acaf062cac
md"""
## Step 1 - Read (some of) the data

The first step in the pipeline is to read the relevant data.
Some of the information required to the model will be read (or created) later.

!!! warning
	The data used here is based on the existing Norse data. When we use a more realistic data source, this section might change heavily.
"""

# ╔═╡ 29b2fb1f-dfa6-4360-90b0-0c01d1ad435b
md"""
### Step 1.1 - What data is expected now

At the current state of the pipeline, we are expecting the following data to be available (although not everything will be used yet):

**Graph data (or geographical data)**: What are the assets and how they connect.
It is not important where they are on a map, but rather their abstract representation.
The nodes of this graph are the **assets** and the edges are the **flows**.

**Assets and flows metadata**: Relevant information for each of the assets and for each of the flows. For instance, the capacity of the flow between nodes A and B.

**Profile data per type**: Profiles that will used (and possibly reused) in assets and flows. These are named and organized according to their types (e.g., availability, demand).

**Assets and flows profiles**: Link the assets and flows to profiles types and names. E.g., asset A uses profile P1 for demand.
"""

# ╔═╡ f6a9c905-cf01-40be-8ad3-71288b5b4929
md"""
### Step 1.2 - Reading data from CSV folder
"""

# ╔═╡ 3bd01539-abac-44ae-8b17-eccb22d1c966
md"""
Reading all CSV files from the `data` folder. Rerun below whenever there are file changes.
"""

# ╔═╡ 38f7f8cb-9f64-47be-8d9e-91f24091c341
begin
	connection = DBInterface.connect(DDB.DB)
	
	for filename in readdir("data")
		if !endswith(".csv")(filename)
			continue
		end
		table_name, _ = splitext(filename)
		table_name = replace(table_name, "-" => "_")
		TIO.create_tbl(connection, "data/$filename"; name = table_name)
	end

	# Inspect initial tables
	DDB.execute(connection, "SHOW TABLES") |> DataFrame
end

# ╔═╡ 2d70eadf-daaa-4409-9126-8b54cfdaa525
md"""
!!! tip "TODO TulipaIO"
	We can create a function that reads all CSV from a folder
"""

# ╔═╡ 84ff13ef-4b9a-4396-a37c-770d82ee5933
md"""
#### Graph and metadata

Both the graph and the metadata are stored in `assets_data` and `flows_data`.
"""

# ╔═╡ f3e981dc-9647-455f-84a1-b623dc522fff
DDB.execute(connection, "SELECT * FROM assets_data") |> DataFrame

# ╔═╡ 9114c0bf-4848-4365-9333-b275ef8fd285
DDB.execute(connection, "SELECT * FROM flows_data") |> DataFrame

# ╔═╡ 6ad02eab-d565-47f4-9468-6182f64fc93a
md"""
#### Profiles data

These profiles include the values of an year hourly.

!!! tip "TODO TulipaClustering"
	Rename asset and time_step to sync with TEM.

!!! tip "TODO"
	The profile data was randomly generated, we should use better data.

!!! tip "TODO"
	Decide how we want to handle profile-type + profily_name and make it uniform.
	Given that the clustering must happen in a single profiles file, it might make sense to keep them all together.
"""

# ╔═╡ 59c4f23d-55e5-4375-b9b1-d08b9c5a40d0
DDB.execute(connection, "SELECT * FROM all_profiles") |> DataFrame

# ╔═╡ e9f8d407-0265-4783-b63d-934ff7a0cc7b
DDB.execute(connection, "SELECT * FROM all_profiles WHERE asset = 'availability-Asgard_Solar'") |> DataFrame

# ╔═╡ 1af1109f-dc99-4d67-bcfc-d4647aa25c1b
DDB.execute(connection, "SELECT DISTINCT ON(asset) * FROM all_profiles") |> DataFrame

# ╔═╡ 9ebf99c7-4296-4541-95f0-0a5b84c2488a
md"""
#### Assets and flows profile linking

This information is not used at all before clustering, although it links to the current profiles. It will be used as is when the profiles are clustered by just pointing to the equivalent representative period profile.
"""

# ╔═╡ 5a82498f-777d-4610-945b-c93bd0e06ffe
DDB.execute(connection, "SELECT * FROM assets_profiles") |> DataFrame

# ╔═╡ 46cde502-2a6d-4c8c-aba7-2c76bc03c3ef
DDB.execute(connection, "SELECT * FROM flows_profiles") |> DataFrame

# ╔═╡ b874f098-aa50-4d93-8c42-56126f6f6153
md"""
## Step 2 - TulipaClustering

The basic idea here it to take the 8760 periods of the profiles, split into 365 equal periods of 24h, then represent each of these periods using a number of representative periods.

The representatives are computed using the whole profiles information, so all profiles need to be in the same table.

!!! tip "TODO"
	Does it make sense to make the `split_into_periods!` function execute at the table level? It would run before creating the dataframes, and thus hopefully it would be faster.
"""

# ╔═╡ 9cd57f33-9906-4f29-af51-b30a688cade6
begin
	period_duration = 24 # will be used later
	tc_df = DataFrame(
		DBInterface.execute(connection, "SELECT * FROM all_profiles")
	)
	TC.split_into_periods!(tc_df; period_duration = period_duration)
	nothing
end

# ╔═╡ 8ace55c3-9ac9-4c25-b4d5-364e5d33dd1b
# Example asset
DF.subset(tc_df, :asset => DF.ByRow(==("availability-Asgard_Solar")))

# ╔═╡ df1a421e-7d54-48a5-9f91-14175e0fd596
# Example asset
DF.subset(tc_df, [:asset,:period] => DF.ByRow((a,p) -> a == "availability-Asgard_Solar" && p == 1))

# ╔═╡ 2d123f6b-1d94-425a-b37b-acc71e035519
begin
	clustering_result = TC.find_representative_periods(tc_df, 35)
	nothing
end

# ╔═╡ e0a815ad-de49-459e-b5e3-72e9418a9283
clustering_result.weight_matrix

# ╔═╡ c409d91a-0938-40d0-88dc-9df4fd8a980f
clustering_result.profiles

# ╔═╡ 7970c2ca-c828-4f83-99e3-18e4ed816b62
DF.subset(
	clustering_result.profiles,
	:asset => DF.ByRow(==("availability-Asgard_Solar"))
)

# ╔═╡ 8e5d990c-8d56-47ee-b1df-b6d375586241
DF.subset(
	clustering_result.profiles,
	[:asset,:rep_period] => DF.ByRow(
		(a, rp) -> a == "availability-Asgard_Solar" && rp == 1
	)
)

# ╔═╡ eb88c4aa-6c15-4a4e-a05e-951fa39a49e1
md"""
!!! tip "TODO"
	After we decide on merging `profile_type` + `profile_name`, it might make sense to move this clustering result to DuckDB directly.
"""

# ╔═╡ 202cca05-e73b-43ed-a126-a52d43516741
md"""
### Step 2.2 - Export to TEM format

We are splitting the profile type and name for TulipaEnergyModel.
This is necessary in the current implementation of TC and TEM.
"""

# ╔═╡ 762be8a2-449e-4e55-aa0f-1e8a6860abf1
md"""
#### `rep_periods_*`

!!! tip "TODO"
	The `rep_periods_data` below is actually input information for the clustering. It could have been created earlier on and used explicitly as input so we don't have to create some loose variables.
"""

# ╔═╡ 94fea98b-9f16-48ab-97fc-aa2467a7ced2
let
	DDB.register_data_frame(
		connection,
		TC.weight_matrix_to_df(clustering_result.weight_matrix),
		"rep_periods_mapping",
	)
	num_rep_periods = size(clustering_result.weight_matrix, 2)
	DDB.register_data_frame(
		connection,
		DataFrame(
			:id => 1:num_rep_periods,
			:num_timesteps => period_duration,
			:resolution => 1.0,
		),
		"rep_periods_data",
	)
end

# ╔═╡ cbaa11c1-4b0f-462d-ab80-e2e653113769
DBInterface.execute(connection, "SELECT * FROM rep_periods_data") |> DataFrame

# ╔═╡ 197f49e9-9602-45c5-aa03-eb6bdd6c607b
DBInterface.execute(connection, "SELECT * FROM rep_periods_mapping") |> DataFrame

# ╔═╡ 52c41422-5e4d-4c0f-b9a8-7d91fbe6a811
md"""
#### `profiles_rep_periods_*`

This is just a split, subset and rename.
"""

# ╔═╡ 916ceea7-86a8-4714-aa37-06bf132bd80b
clustering_result.profiles

# ╔═╡ 2a8683c0-7a48-4157-8c48-fb5cdaf2fcf2
let
	profiles = clustering_result.profiles
	tc_assets = unique(profiles.asset)
	profile_types = getindex.(split.(tc_assets, "-"), 1) |> unique

	for profile_type in profile_types
		DBInterface.execute(connection, "DROP VIEW IF EXISTS profiles_rep_periods_$profile_type")
		df = DF.subset(
			profiles,
			:asset => DF.ByRow(startswith(profile_type))
		) |> df -> DF.combine(df,
			:asset => DF.ByRow(a -> split(a, "-")[2]) => :profile_name,
			:rep_period,
			:time_step => :timestep,
			:value,
		)
		DDB.register_data_frame(connection, df, "profiles_rep_periods_$profile_type")
	end
end

# ╔═╡ f419e9fe-962c-4377-b56c-41b6bb3483e4
DBInterface.execute(connection, "SELECT * FROM profiles_rep_periods_demand LIMIT 5") |> DataFrame

# ╔═╡ afa3f7b5-5d1a-4b75-8dcf-acace2c09b05
md"""
#### `*_rep_periods_profiles`

Let's just create a view because they have the same content.

!!! warning "TODO"
	Make TEM use the original names.
"""

# ╔═╡ 653c44c0-3edc-41be-bbe0-76cf46590cdc
let
	DBInterface.execute(connection, "CREATE VIEW assets_rep_periods_profiles AS SELECT * FROM assets_profiles")
	DBInterface.execute(connection, "CREATE VIEW flows_rep_periods_profiles AS SELECT * FROM flows_profiles")
end

# ╔═╡ 6b73622b-ad36-45eb-8005-bc23c9555f68
md"""
## Step 3 - TulipaEnergyModel

We finally start with TEM.
"""

# ╔═╡ 1ad97d2f-1716-47ea-a300-406d270fbe6f
md"""
### Step 3.1 - User provided info
"""

# ╔═╡ bbcca6dd-45d1-4d70-a0a3-cd91b8a1570b
md"""
#### `assets_timeframe_profiles`
"""

# ╔═╡ 10dbc445-3e9b-45a6-a67a-3469faf0106d
let
	DDB.register_data_frame(
		connection,
		DataFrame(
			:asset => String[],
			:profile_type => String[],
			:profile_name => String[]
		),
		"assets_timeframe_profiles",
	)
end

# ╔═╡ dc52d2f3-6f24-4dd8-ac38-09bc94afe012
md"""
#### Partitions
"""

# ╔═╡ 6adb5dba-6a2a-44e3-b1e9-98e6f40beae4
let
	DDB.register_data_frame(
		connection,
		DataFrame(
			:asset => String[],
			:rep_period => Int[],
			:specification => String[],
			:partition => String[],
		),
		"assets_rep_periods_partitions",
	)
	DDB.register_data_frame(
		connection,
		DataFrame(
			:from_asset => String[],
			:to_asset => String[],
			:rep_period => Int[],
			:specification => String[],
			:partition => String[],
		),
		"flows_rep_periods_partitions",
	)
	DDB.register_data_frame(
		connection,
		DataFrame(
			:asset => String[],
			:specification => String[],
			:partition => String[],
		),
		"assets_timeframe_partitions",
	)
end

# ╔═╡ 05ab44cd-7f47-4a57-9124-d5d77dcca529
md"""
### Step 3.2 - Creating and solving the model
"""

# ╔═╡ 3349642c-c466-4443-a75e-93a477ce4f17
begin
	energy_problem = TEM.EnergyProblem(connection)
	TEM.create_model!(energy_problem)
	TEM.solve_model!(energy_problem)
end

# ╔═╡ a029d632-9541-4211-afea-c53fe2bf2908
md"""
## Step 4 - Plotting the solution
"""

# ╔═╡ 638aad2d-2c40-475c-81de-67696e4fef0c
md"""
---

## Behind the scenes
"""

# ╔═╡ 3bd6c7aa-ea9d-4e95-9c98-d2ed245f8600
let
	function nicename(filename)
		filename = replace(filename, "profiles-rep-periods-" => "")
		filename = replace(filename, "-" => "_")
		filename, _ = splitext(filename)
		filename
	end
	function read_csv(filename)
		CSV.read(joinpath("Norse-from-TEM", filename), DataFrame; header=2)
	end
	# For debugging
	# rp_data = read_csv("rep-periods-data.csv")
	# rp_map = read_csv("rep-periods-mapping.csv")
	# profiles = Dict(
	# 	nicename(filename) => read_csv(filename)

	# 	for filename in readdir("Norse-from-TEM") if startswith("profiles-rep")(filename)
	# )
	# End debugging

	df_TC_profiles = DataFrame(:asset => String[], :time_step => Int[], :value => Float64[])
	for filename in readdir("Norse-from-TEM")
		if !startswith("profiles-rep")(filename)
			continue
		end
		profile_type = nicename(filename)
		df = read_csv(filename)
		profile_names = unique(df.profile_name)
		# Ignore all content and generate fake data for the year
		for profile_name in profile_names
			append!(df_TC_profiles,
				DataFrame(
					:asset => fill("$profile_type-$profile_name", 365 * 24),
					:time_step => 1:365 * 24,
					:value => rand(365 * 24),
				)
			)
		end
	end
	filename = joinpath("data", "all-profiles.csv")
	open(filename, "w") do io
		println(io, ",,p.u.")
	end
	CSV.write(
		joinpath("data", "all-profiles.csv"),
		df_TC_profiles,
		append=true,
		writeheader=true,
	)
end

# ╔═╡ 748da88b-809b-4082-a871-9136263e5055


# ╔═╡ bb7d7220-fb0a-49ff-8bcf-9331b76731e5


# ╔═╡ Cell order:
# ╠═6876e975-7525-4139-803b-7bc7d939d6b5
# ╠═5e8526b5-f13c-45a5-9d61-8acdfa014f50
# ╠═1d505b9e-31fd-11ef-1f7a-2171b11023ea
# ╟─a46312ec-7cc7-432b-8965-a82f4a163a4b
# ╟─75094888-27c7-430c-8a77-38d104b5b389
# ╟─e4688e71-118a-40e6-a060-18acaf062cac
# ╟─29b2fb1f-dfa6-4360-90b0-0c01d1ad435b
# ╟─f6a9c905-cf01-40be-8ad3-71288b5b4929
# ╟─3bd01539-abac-44ae-8b17-eccb22d1c966
# ╠═38f7f8cb-9f64-47be-8d9e-91f24091c341
# ╟─2d70eadf-daaa-4409-9126-8b54cfdaa525
# ╟─84ff13ef-4b9a-4396-a37c-770d82ee5933
# ╠═f3e981dc-9647-455f-84a1-b623dc522fff
# ╠═9114c0bf-4848-4365-9333-b275ef8fd285
# ╠═6ad02eab-d565-47f4-9468-6182f64fc93a
# ╠═59c4f23d-55e5-4375-b9b1-d08b9c5a40d0
# ╠═e9f8d407-0265-4783-b63d-934ff7a0cc7b
# ╠═1af1109f-dc99-4d67-bcfc-d4647aa25c1b
# ╟─9ebf99c7-4296-4541-95f0-0a5b84c2488a
# ╠═5a82498f-777d-4610-945b-c93bd0e06ffe
# ╠═46cde502-2a6d-4c8c-aba7-2c76bc03c3ef
# ╟─b874f098-aa50-4d93-8c42-56126f6f6153
# ╠═9cd57f33-9906-4f29-af51-b30a688cade6
# ╠═8ace55c3-9ac9-4c25-b4d5-364e5d33dd1b
# ╠═df1a421e-7d54-48a5-9f91-14175e0fd596
# ╠═2d123f6b-1d94-425a-b37b-acc71e035519
# ╠═e0a815ad-de49-459e-b5e3-72e9418a9283
# ╠═c409d91a-0938-40d0-88dc-9df4fd8a980f
# ╠═7970c2ca-c828-4f83-99e3-18e4ed816b62
# ╠═8e5d990c-8d56-47ee-b1df-b6d375586241
# ╠═eb88c4aa-6c15-4a4e-a05e-951fa39a49e1
# ╠═202cca05-e73b-43ed-a126-a52d43516741
# ╟─762be8a2-449e-4e55-aa0f-1e8a6860abf1
# ╠═94fea98b-9f16-48ab-97fc-aa2467a7ced2
# ╠═cbaa11c1-4b0f-462d-ab80-e2e653113769
# ╠═197f49e9-9602-45c5-aa03-eb6bdd6c607b
# ╟─52c41422-5e4d-4c0f-b9a8-7d91fbe6a811
# ╠═916ceea7-86a8-4714-aa37-06bf132bd80b
# ╠═2a8683c0-7a48-4157-8c48-fb5cdaf2fcf2
# ╠═f419e9fe-962c-4377-b56c-41b6bb3483e4
# ╟─afa3f7b5-5d1a-4b75-8dcf-acace2c09b05
# ╠═653c44c0-3edc-41be-bbe0-76cf46590cdc
# ╠═6b73622b-ad36-45eb-8005-bc23c9555f68
# ╠═1ad97d2f-1716-47ea-a300-406d270fbe6f
# ╠═bbcca6dd-45d1-4d70-a0a3-cd91b8a1570b
# ╠═10dbc445-3e9b-45a6-a67a-3469faf0106d
# ╠═dc52d2f3-6f24-4dd8-ac38-09bc94afe012
# ╠═6adb5dba-6a2a-44e3-b1e9-98e6f40beae4
# ╠═05ab44cd-7f47-4a57-9124-d5d77dcca529
# ╠═3349642c-c466-4443-a75e-93a477ce4f17
# ╠═a029d632-9541-4211-afea-c53fe2bf2908
# ╠═638aad2d-2c40-475c-81de-67696e4fef0c
# ╠═3bd6c7aa-ea9d-4e95-9c98-d2ed245f8600
# ╠═748da88b-809b-4082-a871-9136263e5055
# ╠═bb7d7220-fb0a-49ff-8bcf-9331b76731e5
