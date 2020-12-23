package com.thelearningjournals.spark

import org.apache.spark._
import org.apache.log4j._


object mypopularsuperherordd {
    Logger.getLogger("org").setLevel(Level.ERROR)
  //my function to split the superhero and other superheroes from every line
   def get_main_super_here_and_other_heroes(line:String): (Int, Int) ={
      val superhero_array = line.split(" ")
      val main_hero = superhero_array(0).toInt
      val other_heros_count = superhero_array.size -1
     return(main_hero,other_heros_count)
   }

    def get_id_and_name(line:String):(Int,String)={
       val pattern = """(\d+) (.*)""".r
        val res = pattern.findAllIn(line)
        val hero_id = res.group(1).toInt
        val super_hero_name = res.group(2).toString
       return(hero_id,super_hero_name)
    }


    def main(args: Array[String]): Unit ={
       //load the data of superhero from marvel-graph.txt
       val sc = new SparkContext("local[*]","mysparksuperhero")
      val mySuperHeroRdd = sc.textFile("data/Marvel-graph.txt")
      val SuperheroRdd = mySuperHeroRdd.map(get_main_super_here_and_other_heroes)
      val finalsuperhero = SuperheroRdd.reduceByKey((x,y)=>(x+y))
      val fhero = finalsuperhero.map(x=>(x._2,x._1)).sortByKey(ascending = false).take(1)
      val h_id = fhero(0)._2
      val chero_count = fhero(0)._1
      val superheronamerdd = sc.textFile("data/Marvel-names.txt")
      val superheroname = superheronamerdd.map(get_id_and_name)
      val a = superheroname.filter(x=>x._1==h_id).collect()
      for(x<-a){
          val hero = x._2
          printf(s"Most popular superhero is $hero having co-occurences with total $chero_count superheros")
      }



    }

}
