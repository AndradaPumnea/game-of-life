package de.tuberlin.dima.bdapro.solutions.gameoflife;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import javafx.scene.SubScene;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class GameOfLifeTaskImpl implements GameOfLifeTask {


	static int n, m;
	@Override
	public int solve(String inputFile, int argN, int argM, int numSteps) throws Exception {
		//******************************
		//*Implement your solution here*
		//******************************

		n = argN;
		m = argM;
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();




    	DataSet<String> text = env.readTextFile(inputFile);

		DataSet<Tuple2<Cell, Integer>> firstGen = text.flatMap(new LineSplitter());

		//firstGen.print();

		IterativeDataSet<Tuple2<Cell, Integer>> iteration = firstGen.iterate(numSteps);

		DataSet<Tuple2<Cell, Integer>> aliveCells = iteration.reduceGroup(new CountNeighboursAlive())
				.filter(new DeathFilter());


		DataSet<Tuple2<Cell, Integer>> bornCells = iteration.reduceGroup(new BornCells())
				.reduceGroup(new CountNeighboursBorn())
				.filter(new BornFilter());

		DataSet<Tuple2<Cell, Integer>> newGen = aliveCells.union(bornCells);

		DataSet<Tuple2<Cell, Integer>> result = iteration.closeWith(newGen);

		System.out.println(result.collect().size());
		return result.collect().size();
	}

	public static final class LineSplitter implements FlatMapFunction<String, Tuple2<Cell, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<Cell, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.split("\\s+");
//			System.out.println("token0 " + tokens[0]);
//			System.out.println("token1 " + tokens[1]);
			out.collect(new Tuple2<Cell, Integer>( new Cell(Long.parseLong(tokens[0]),Long.parseLong(tokens[1])), 0));
		}
	}

	public class CountNeighboursAlive implements GroupReduceFunction<Tuple2<Cell, Integer>, Tuple2<Cell, Integer>> {

		@Override
		public void reduce(Iterable<Tuple2<Cell, Integer>> values, Collector<Tuple2<Cell, Integer>> out) {

			int neighbours;
			List<Cell> aliveCells = new ArrayList<>();

			for (Tuple2<Cell, Integer> k : values){
					aliveCells.add(k.f0);
			}

			List<Integer> normal = new ArrayList<>(Arrays.asList(-1,-0,1));
			List<Integer> circularLeftX= new ArrayList<>(Arrays.asList(n-1,0,1));
			List<Integer> circularRightX = new ArrayList<>(Arrays.asList(-1,0,1-n));
			List<Integer> circularLeftY= new ArrayList<>(Arrays.asList(m-1,0,1));
			List<Integer> circularRightY = new ArrayList<>(Arrays.asList(-1,0,1-m));

			List<Integer> firstFor;
			List<Integer> secondFor;

			for(Cell k : aliveCells){
				if(k.getRow() == 0)
					firstFor = circularLeftX;
				else if (k.getRow() == n-1)
					firstFor = circularRightX;
				else
					firstFor = normal;

				if(k.getColumn() == 0)
					secondFor = circularLeftY;
				else if (k.getColumn() == m-1)
					secondFor = circularRightY;
				else
					secondFor = normal;

				neighbours = 0;
				for(int i : firstFor){
					for(int j : secondFor){
						if(i == 0 && j == 0){
							continue;
						}
						if(aliveCells.contains(new Cell(k.getRow()+i, k.getColumn()+j))){
							neighbours++;
						}
					}
				}
				out.collect(new Tuple2<Cell, Integer>(k, neighbours));
			}
		}
	}

	public class CountNeighboursBorn implements GroupReduceFunction<Tuple2<Cell, Integer>, Tuple2<Cell, Integer>> {

		@Override
		public void reduce(Iterable<Tuple2<Cell, Integer>> values, Collector<Tuple2<Cell, Integer>> out) {

			int neighbours;
			List<Cell> aliveCells = new ArrayList<>();
			List<Cell> bornCells = new ArrayList<>();

			for (Tuple2<Cell, Integer> k : values){
				if (k.f0.getState() == true){
					aliveCells.add(k.f0);
				} else {
					bornCells.add(k.f0);
				}

			}

			List<Integer> normal = new ArrayList<>(Arrays.asList(-1,-0,1));
			List<Integer> circularLeftX= new ArrayList<>(Arrays.asList(n-1,0,1));
			List<Integer> circularRightX = new ArrayList<>(Arrays.asList(-1,0,1-n));
			List<Integer> circularLeftY= new ArrayList<>(Arrays.asList(m-1,0,1));
			List<Integer> circularRightY = new ArrayList<>(Arrays.asList(-1,0,1-m));

			List<Integer> firstFor;
			List<Integer> secondFor;

			for(Cell k : bornCells){
				if(k.getRow() == 0)
					firstFor = circularLeftX;
				else if (k.getRow() == n-1)
					firstFor = circularRightX;
				else
					firstFor = normal;

				if(k.getColumn() == 0)
					secondFor = circularLeftY;
				else if (k.getColumn() == m-1)
					secondFor = circularRightY;
				else
					secondFor = normal;

				neighbours = 0;
				for(int i : firstFor){
					for(int j : secondFor){
						if(i == 0 && j == 0){
							continue;
						}
						if(aliveCells.contains(new Cell(k.getRow()+i, k.getColumn()+j, true))){
							neighbours++;
						}
					}
				}
				out.collect(new Tuple2<Cell, Integer>(k, neighbours));
			}
		}

	}

	public class BornCells implements GroupReduceFunction<Tuple2<Cell, Integer>, Tuple2<Cell, Integer>> {

		@Override
		public void reduce(Iterable<Tuple2<Cell, Integer>> values, Collector<Tuple2<Cell, Integer>> out) {

			List<Cell> coordinates = new ArrayList<>();

			List<Integer> normal = new ArrayList<>(Arrays.asList(-1,0,1));
			List<Integer> circularLeftX= new ArrayList<>(Arrays.asList(n-1,0,1));
			List<Integer> circularRightX = new ArrayList<>(Arrays.asList(-1,0,1-n));
			List<Integer> circularLeftY= new ArrayList<>(Arrays.asList(m-1,0,1));
			List<Integer> circularRightY = new ArrayList<>(Arrays.asList(-1,0,1-m));

			List<Integer> firstFor;
			List<Integer> secondFor;

			for(Tuple2<Cell, Integer> k : values){
				if(k.f0.getRow() == 0)
					firstFor = circularLeftX;
				else if (k.f0.getRow() == n-1)
					firstFor = circularRightX;
				else
					firstFor = normal;

				if(k.f0.getColumn() == 0)
					secondFor = circularLeftY;
				else if (k.f0.getColumn() == m-1)
					secondFor = circularRightY;
				else
					secondFor = normal;

				for(int i : firstFor)
					for(int j : secondFor){
						if(i == 0 && j == 0){
							coordinates.remove(new Cell(k.f0.getRow()+ i,k.f0.getColumn() + j,false));
							coordinates.add(new Cell(k.f0.getRow()+ i,k.f0.getColumn() + j,true));
							continue;
						}
						Cell c = new Cell(k.f0.getRow() + i,k.f0.getColumn() + j,false);
						if(!coordinates.contains(c) && !coordinates.contains(new Cell(k.f0.getRow() + i,k.f0.getColumn() + j,true))){
							coordinates.add(c);
						}
					}
			}

			for (Cell cell: coordinates){
				out.collect(new Tuple2<Cell, Integer>(cell, 0));
			}

		}
	}

	public class DeathFilter implements FilterFunction<Tuple2<Cell, Integer>> {

		@Override
		public boolean filter(Tuple2<Cell, Integer> cell) {
			if (cell.f1 > 3 || cell.f1 < 2){
				return false;
			}
			return true;
		}
	}

	public class BornFilter implements FilterFunction<Tuple2<Cell, Integer>> {

		@Override
		public boolean filter(Tuple2<Cell, Integer> cell) {
			return cell.f1 == 3;
		}
	}


}