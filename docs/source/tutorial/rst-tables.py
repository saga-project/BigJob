import os
import sys

def main():
    array = [ 
        [
            'Resource (Hostname)', 
            'SAGA Adaptor', 
            'Queue Names',  
            'Number of Processes', 
            'Max Walltime (if known)',
            'Project Accounts'
        ],
	[ 
            'All machines',
            'fork, ssh',
            'N/A',
            'Depends on machine cores',
            'N/A',
            'N/A'
        ],
	[ 
            'stampede.tacc.utexas.edu',
            ['Local: slurm', 'Remote: slurm+ssh', 'slurm+gsissh'], 
            ['normal', 'development', 'serial', 'largemem'],
            ['4K (increments of 16)','256 (increments of 16)','16','128 (increments of 32)'], 
            ['48hrs','4hrs','12hrs','24hrs','48hrs'],
            'XSEDE Allocation'
        ],
	[ 
            'lonestar.tacc.utexas.edu',
            ['Local: sge', 'Remote: sge+ssh', 'sge+gsissh'], 
            ['normal', 'development', 'largemem'],
            ['4K (increments of 12)','256 (increments of 12)','128 (increments of 24)'], 
            ['48hrs','4hrs','48hrs'],
            'XSEDE Allocation' 
        ],
	[ 
            'trestles.sdsc.edu',
            ['Local: pbs', 'Remote: pbs+ssh', 'pbs+gsissh'], 
            ['normal', 'shared'],
            ['1024 (increments of 32)','128 (increments of 32)'], 
            ['48hrs','48hrs'],
            'XSEDE Allocation' 
        ]

    ]
    rst = make_table(array)
    print rst
    return(0)

def make_table(grid):

    num_rows = len(grid)
    num_cols = len(grid[0])

    width_cols = []
    for i in range(0, num_cols) :
        print i
        width_cols.append (0)
        for j in range(0, num_rows) :
            elem     = grid[j][i]
            elem_len = 0
            print "%d - %d: %s" % (i, j, elem)
            if  type(elem) is list :
                for entry in elem :
                    elem_len = max (elem_len, len(entry))
            else : # assume string
                elem_len = max (elem_len, len(elem))
                
            width_cols[i] = max (width_cols[i], elem_len+2)
    
    rst = table_div (width_cols, '-')

    header_char = '='
    for i in range(0, num_rows) :
        row        = grid[i]
        elems      = []
        max_nelems = 1
        for j in range(0, num_cols) :
            elem = row[j]
            if  type(elem) is list :
                max_nelems = max (max_nelems, len(elem))

        for k in range (0, max_nelems) :
            for j in range(0, num_cols) :
                elem = row[j]
                val = "--"
                if  type(elem) is list :
                    if len(elem) > k :
                        val = elem[k]
                else :
                    if  k == 0 :
                        val = elem
                fmt = "%%-%ds" % width_cols[j]
                print fmt
                rst += '| ' + fmt % str(val) + ' '

            rst += '|\n'
          # if  type(elem) is list :
          #     for entry in elem :
          #         elem_len = max (elem_len, len(entry))
          # else : # assume string
          #     elem_len = max (elem_len, len(elem))
          #
    #     rst = rst + '| ' + '| '.join([normalize_cell(x, cell_width-1) for x in row]) + '|\n'
        rst = rst + table_div(width_cols, header_char)
        header_char = '-'
    return rst


def table_div(width_cols, char):

    ret = ""

    for width in width_cols :
        ret += "+" + (width+2) * char

    return ret + "+\n"


def normalize_cell(string, length):
    return string + ((length - len(string)) * ' ')

if __name__ == "__main__":
    sys.exit(main())

# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

