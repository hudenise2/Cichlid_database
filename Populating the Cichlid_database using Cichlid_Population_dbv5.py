Populating the Cichlid_database using Cichlid_Population_dbv5.py
This script imports data from 4 sources into a mysql database for R. Durbin's group cichlid database.
The data are from:
- json file generated from Sanger sequencing database, containing sequencing information
- tsv file extracted from Sanger sequencing warehouse facility, containing metadata and project information
- Google fusion table from Cambridge Cichlid website (https://cambridgecichlids.org) containing photo information
- 2 Google spreadsheet containing metadata and sequencing details (https://docs.google.com/spreadsheets/d/1eoVGpkX--R5FvFzj9jic8uiDoS-i9K6m3dY3_C4X96Y/edit?ts=5bc7080f, sequenced-master and sample-master tabs).

The script performs some curation of the data. However some cleaning-up of the data is necessary afterwards.
Here are the steps to populate the database:
1- Population ontology table
insert into ontology (name, ontology_name) values ('juvenile', 'UBERON:0034919');
2- Insert data into provider
insert into provider (provider_name, email, affiliation, address, phone, changed, latest) values ('Hannes Svardal','hxxxx@xxxx.be','University of Antwerp', 'Campus Groenenborger Groenenborgerlaan 171, G.U.758, 2020 Antwerpen, Belgium','0032 xxxxxxxx','2019-01-03',1)
insert into provider (provider_name, email, affiliation, address, phone, changed, latest) values ('Milan Malinsky','mxxxxxx','University of Basel','Zoological Institute, Vesalgasse 1, CH-4051 Basel, Switzerland','0041 xxxxxxx','2019-01-07',1)
3- Upload data from json file
./Cichlid_Population_dbv5.py -v -j /Users/hd/Documents/Cichlid_database/data/cichlids_iRods.json -o
4- Insert data from master spreadsheet
./Cichlid_Population_dbv5.py -sp samples -o
./Cichlid_Population_dbv5.py -sp sequenced -o
./Cichlid_Population_dbv5.py -sp mlw -o
5- Update data according to Milan Malinsky’s curation (he found that some species and alias were mislabelled)
individual_name   wrong_alias   correct_species_name    correct_alias_name
D23-D10. 	LabFul1	   Labeotropheus fuelleborni		LabFue28
D02-C04		         Copadichromis quadrimaculatus
D13.D04		                  Melanochromis auratus
6- Upload data from cambridgeciclids.org website
./Cichlid_Population_dbv5.py -sp images -o
7- Insert sequence information for sequencing centre
insert into seq_centre (name) values ('Wellcome Trust Sanger Institute’);
Update lane set seq_centre_id  = 1 where name is not null;
8- Taxonomy curation
To deal with inconsistencies in the species names/common names and misspelled species names:
species_id    species_name      taxon_id     common_name
369	Tramitichromis intermedius	323801	Tramitichromis intermedius
25	Astatotilapia "rujewa"	NULL	NULL
383	Astatotilapia rujewa	NULL	Astatotilapia rujewa
23	Aulonocara "gold"	NULL	NULL
31	Aulonocara "minutus"	NULL	NULL
30	Aulonocara "yellow"	NULL	NULL
250	Aulonocara brevirostris yellow	NULL	NULL
370	Aulonocara brevirostrus yellow	NULL	Aulonocara sp. brevirostrus yellow
366	Aulonocara macrocheir	NULL	Aulonocara macrocheir
244	Aulonocara macrochir	NULL	NULL
384	Aulonocara minutus	NULL	Aulonocara minutus
401	Aulonocara sp. "gold"	163613	NULL
403	Aulonocara sp. "yellow"	163614	NULL
136	Aulonocara yellow	NULL	Aulonocara sp. yellow
19	Champsochromis caeruelus	NULL	NULL
360	Champsochromis caeruleus	NULL	Champsochromis caeruelus
372	Champsochromis ceruleus	NULL	NULL
359	check specimen check specimen	NULL	check specimen check specimen
117	Cyrtocara moori	30882	hump-head
414	Cyrtocara moorii	30882	hump-head
362	Dimidiochromis compressiceps	106584	Malawi eyebiter
26	Dimidiochromis compressiceps (Malawi eyebiter)	NULL	NULL
57	Dimidiochromis kiwinge	163619	Dimidiochromis kiwinge
169	Diplotaxodon argenteus large	NULL	NULL
358	Diplotaxodon limnothrissa black pelvic	NULL	Diplotaxodon sp. limnothrissa black pelvic
99	Diplotaxodon limnothrissa black pelvis	NULL	NULL
111	Diplotaxodon similis	165732	Diplotaxodon similis
413	Diplotaxodon sp. "similis"	165732	NULL
355	empty empty	NULL	empty empty
92	Fossochromis rostratus	NULL	Fossochromis rostratus
12	Fossorochromis rostratus	137271	Fossorochromis rostratus
37	Hemitaenichromis spilopterus	NULL	NULL
14	Hemitaeniochromis spilopterus	NULL	Hemitaenichromis spilopterus
76	Homo sapiens	9606	human
127	Labeotropheus fuelleborni	57307	blue mbuna
380	Labeotropheus fulleborni	NULL	NULL
126	Labeotropheus trewavasae	120210	Labeotropheus trewavasae
13	Labeotropheus trewavasae (scrapermouth mbuna)	NULL	NULL
264	Labeotropheus trewawase	NULL	NULL
356	Labidochromis caeruleus	50897	blue streak hap
119	Labidochromis ceraleus	NULL	NULL
351	Lethrinops oliveri	1075429	Lethrinops oliveri
402	Lethrinops sp. "oliveri"	165733	NULL
157	Lethrinops unknown	NULL	NULL
153	Lethrinops unkown	NULL	NULL
219	Melanochromis dialeptos	NULL	NULL
363	Melanochromis dileptos	NULL	Melanochromis dileptos
365	Melanochromis johanni	28812	Melanochromis johanni
224	Melanochromis johannii	28812	NULL
342	Nimbochromis livingstoni	137275	Nimbochromis livingstoni
220	Nimbochromis livingstonii	137275	Nimbochromis livingstonii
11	Placidochromis subocularis	1075433	Placidochromis subocularis
378	Placidochromis subocularis?	1075433	NULL
265	Protomelas fenestratus	120225	Protomelas fenestratus
379	Protomelas fenestratus?	120225	NULL
280	Protomelas labridens	NULL	NULL
354	Protomelas labroides	NULL	Protomelas labroides
91	Protomelas similis	29149	red empress cichlid
374	Protomelas similis ?	29149	red empress cichlid
178	Protomelas triaenodon	NULL	Protomelas c.f. triaenodon
361	Protomelas trianodon	NULL	Protomelas trianodon
172	Pseudotropheus acei	338889	Pseudotropheus acei
192	Pseudotropheus livingstoni	NULL	NULL
125	Pseudotropheus livingstonii	40194	Pseudotropheus livingstonii
430	Pseudotropheus sp. "acei"	338889	NULL
436	Rhamphochromis cf. longiceps "yellow belly"	454635	NULL
102	Rhamphochromis grey	NULL	Rhamphochromis c.f. sp. grey
308	Rhamphochromis long snout	NULL	Rhamphochromis sp. long snout
307	Rhamphochromis longiceps yellow belly	NULL	Rhamphochromis sp. longiceps yellow belly
432	Rhamphochromis sp. "grey"	454632	NULL
437	Rhamphochromis sp. "long snout"	323795	NULL
52	Stigmatochromis "guttatus"	NULL	NULL
260	Stigmatochromis guttatus	NULL	Stigmatochromis guttatus
364	Stigmatochromis philodophorus	NULL	Stigmatochromis philodophorus
240	Stigmatochromis pholidophorus	NULL	NULL
182	Tramitichromis intermedius	323801	Tramitichromis intermedius
156	Trematochranus placodon	NULL	NULL
10	Trematocranus placodon	323800	snail-crusher hap
339	Tremitochranus placodon	NULL	NULL
235	Tropheops black	NULL	Tropheops sp. black
242	Tropheops chiofu 3	NULL	NULL
230	Tropheops chiofu yellow	NULL	NULL
204	Tropheops choifu 3	NULL	Tropheops sp. choifu 3
303	Tropheops choifu yellow	NULL	Tropheops sp. choifu yellow
162	Tropheops mauve	NULL	Tropheops sp. mauve
212	Tropheops olive	NULL	Tropheops sp. olive
278	Tropheops rust	NULL	Tropheops rust
420	Tropheops sp. "black"	57443	NULL
422	Tropheops sp. "mauve"	286600	NULL
412	Tropheops sp. "olive"	323793	NULL
409	Tropheops sp. "rust"	323794	NULL
353	Unknown unknown	NULL	Cyathochromis obliquidens
