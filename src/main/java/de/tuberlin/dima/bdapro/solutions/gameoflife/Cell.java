package de.tuberlin.dima.bdapro.solutions.gameoflife;

public class Cell {

    private boolean state;
    private long row;
    private long column;

    public Cell (long i, long j){
        this.row = i;
        this.column = j;
        this.state = false;
    }

    public Cell (long i, long j, boolean state){
        this.row = i;
        this.column = j;
        this.state = state;
    }

    public long getRow(){
        return this.row;
    }

    public long getColumn(){
        return this.column;
    }

    public boolean getState(){
        return this.state;
    }

    public void updateCell(boolean newState){
        this.state = newState;
    }

    public String toString(){
        return "(" + this.row + ", " + this.column + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Cell cell = (Cell) o;

        if (state != cell.state) return false;
        if (row != cell.row) return false;
        return column == cell.column;
    }

    @Override
    public int hashCode() {
        int result = (state ? 1 : 0);
        result = 31 * result + (int) (row ^ (row >>> 32));
        result = 31 * result + (int) (column ^ (column >>> 32));
        return result;
    }
}
