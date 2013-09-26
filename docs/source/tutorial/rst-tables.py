import os
import sys

def main():
    array = [ ['Resource (Hostname)', 'SAGA Adaptor', 'Queue Names', 'Project Accounts', 'Number of Processes', 'Max Walltime (if known)'],
	      ['All machines','fork, ssh','N/A','N/A','Depends on machine cores','N/A'],
	      ['stampede.tacc.utexas.edu','Local: slurm, Remote: slurm+ssh, slurm+gsissh','normal, development,serial,largemem','XSEDE Allocation', '4K (increments of 16),256 (increments of 16),16,128 (increments of 32)', '48hrs,4hrs,12hrs,24hrs,48hrs'],]
    rst = make_table(array)
    print rst
    return(0)

def make_table(grid):
    cell_width = 2 + max(reduce(lambda x,y: x+y, [[len(item) for item in row] for row in grid], []))
    num_cols = len(grid[0])
    rst = table_div(num_cols, cell_width, 0)
    header_flag = 1
    for row in grid:
        rst = rst + '| ' + '| '.join([normalize_cell(x, cell_width-1) for x in row]) + '|\n'
        rst = rst + table_div(num_cols, cell_width, header_flag)
        header_flag = 0
    return rst

def table_div(num_cols, col_width, header_flag):
    if header_flag == 1:
        return num_cols*('+' + (col_width)*'=') + '+\n'
    else:
        return num_cols*('+' + (col_width)*'-') + '+\n'

def normalize_cell(string, length):
    return string + ((length - len(string)) * ' ')

if __name__ == "__main__":
    sys.exit(main())
