drop view if exists recipe_run_stats_def;
create view recipe_run_stats_def as select `rs`.`stat_id` as `stat_id`,
       `rs`.`RecipeGrpNum` as `RecipeGrpNum`,
       `rs`.`RecipeNum` as `RecipeNum`,
       `rs`.`duration_median` as `duration`,
       `rs`.`offset_median` as `offset`
  from (`recipe_run_stats` `rs`
  join `run_stat` `s`
       on (((`rs`.`stat_id` = `s`.`id`) and (`s`.`default_stat` = 1))));
