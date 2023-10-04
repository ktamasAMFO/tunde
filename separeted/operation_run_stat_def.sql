drop view if exists operation_run_stats_def;
create view operation_run_stats_def as select `rs`.`stat_id` as `stat_id`,
       `rs`.`RecipeGrpNum` as `RecipeGrpNum`,
       `rs`.`RecipeNum` as `RecipeNum`,
       `rs`.`UnitRecipeNum` as `UnitRecipeNum`,
       `rs`.`Op` as `Op`,
       `rs`.`OpInst` as `OpInst`,
       `rs`.`duration_median` as `duration`,
       `rs`.`offset_median` as `offset`,
       `rs`.`total_offset_median` as `total_offset`
  from (`operation_run_stats` `rs`
  join `run_stat` `s`
       on (((`rs`.`stat_id` = `s`.`id`) and (`s`.`default_stat` = 1))));
