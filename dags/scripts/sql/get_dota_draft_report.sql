INSERT INTO public.dota_draft_report (
   match_id,
   radiant_win,
   pick_1,
   pick_2,
   pick_3,
   pick_4,
   pick_5,
   pick_6,
   pick_7,
   pick_8,
   pick_9,
   pick_10,
   insert_date
)

SELECT ddcs.match_id, ddcs.radiant_win,
       h1.localizedname as pick_1,
       h2.localizedname as pick_2,
       h3.localizedname as pick_3,
       h4.localizedname as pick_4,
       h5.localizedname as pick_5,
       h6.localizedname as pick_6,
       h7.localizedname as pick_7,
       h8.localizedname as pick_8,
       h9.localizedname as pick_9,
       h10.localizedname as pick_10,
       '{{ ds }}' as insert_date
FROM spectrum.dota_draft_clean_stage ddcs
LEFT JOIN spectrum.dota_heroes_staging h1 on pick_1 = h1.id
LEFT JOIN spectrum.dota_heroes_staging h2 on pick_2 = h2.id
LEFT JOIN spectrum.dota_heroes_staging h3 on pick_3 = h3.id
LEFT JOIN spectrum.dota_heroes_staging h4 on pick_4 = h4.id
LEFT JOIN spectrum.dota_heroes_staging h5 on pick_5 = h5.id
LEFT JOIN spectrum.dota_heroes_staging h6 on pick_6 = h6.id
LEFT JOIN spectrum.dota_heroes_staging h7 on pick_7 = h7.id
LEFT JOIN spectrum.dota_heroes_staging h8 on pick_8 = h8.id
LEFT JOIN spectrum.dota_heroes_staging h9 on pick_9 = h9.id
LEFT JOIN spectrum.dota_heroes_staging h10 on pick_10 = h10.id;
