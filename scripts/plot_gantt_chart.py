#!/usr/bin/env python2.7
#
# This is a really simple Python script to do quick visualization of
# a schedule. Workflow tasks in red, and background tasks in green.
########################################################################

import random
import matplotlib.patches as patches
import matplotlib.pyplot as pp
import sys



if __name__ == '__main__':
    
    if (len(sys.argv) != 2):
        print >> sys.stderr, "Usage: ./plot_gantt_chart.py <csv file>"
        exit(-1)

    # Read all input
    fname = sys.argv[1]
    with open(fname) as f:
        lines = f.readlines()
        lines = [x.strip() for x in lines]

    # Create a plot
    pp.figure()
    ax = pp.gca()
    pp.xlabel('Time')
    pp.ylabel('Processors')

    min_x = -1
    max_x = -1
    min_y = -1
    max_y = -1

    min_red = 0.15
    max_red = 1.00
    current_red = min_red

    for line in lines: 
        tokens = line.split(",")
        start_time = float(tokens[8])
        finish_time = float(tokens[3])
        desired_color = tokens[5].split(":")[1].split('"')[0]

        if (desired_color == "red"):
            rgb = (current_red, 0.2, 0.2)
            current_red += .17
            if (current_red > max_red):
                current_red = min_red
        else:
            rgb = (0.0, 0.7, 0.0)

        processors = tokens[0]
        proc_meta_tokens = processors.split(' ')
        for meta_token in proc_meta_tokens:
            proc_tokens = meta_token.split('-')
            min_proc = int(proc_tokens[0])
            if len(proc_tokens) == 2:
                max_proc = int(proc_tokens[1])
            else:
                max_proc = int(proc_tokens[0])
    
            #print "Drawing : (", min_proc, ",", start_time, "), width=", (finish_time - start_time), " height=", (max_proc - min_proc), "color=", desired_color
            rect = patches.Rectangle([start_time, min_proc], (finish_time - start_time), (max_proc - min_proc + 1), facecolor=rgb, edgecolor="black", linewidth=0.2)
            ax.add_patch(rect)

            if (min_x == -1) or (min_x > start_time):
                min_x = start_time
            if (max_x == -1) or (max_x < finish_time):
                max_x = finish_time
            if (min_y == -1) or (min_y > min_proc):
                min_y = min_proc
            if (max_y == -1) or (max_y < max_proc + 1):
                max_y = max_proc + 1
    
        
    print min_x, max_x, min_y, max_y

    ax.set_xlim([min_x,max_x])
    ax.set_ylim([min_y,max_y])
    pp.savefig('/tmp/gantt.pdf')
    print "Figure saved in /tmp/gantt.pdf"
