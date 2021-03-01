/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.mavenproject1;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;
import scala.Tuple2;

/**
 *
 * @author kwstas
 */
public class TupleComparator implements Comparator<Tuple2<Double, Double>>, Serializable {

    @Override
    public int compare(Tuple2<Double, Double> o1, Tuple2<Double, Double> o2) {
        if(Objects.equals(o1._1(), o2._1()))
        {
            return (int) (o1._2() - o2._2());
        }else
        {
            return (int) (o1._1() - o2._1());
        }
    }
    
}
