import logging

import tkinter as tk
from tkinter import ttk

class ColumnHeader(tk.Canvas):

    def __init__(self, parent, bg):
        self.height = 20
        super().__init__(parent, bg=bg, width=500, height=self.height, bd=0)
        self.ps = parent.state
        self.config(width=self.ps['width'])

    def render(self, current_col=''):
        self.configure(scrollregion=(0,0, self.ps['width'], self.ps['height']+30))
        self.delete('header-border')
        self.delete('header-text')

        pad = 0
        #print (self.ps['column_width_list'], len(self.ps['columns']))
        for i, (col_key, col) in enumerate(self.ps['columns'].items()):
            x1 = self.ps['column_width_list'][i] + pad
            x2 = self.ps['column_width_list'][i+1] + pad

            x_center = x1 + 4 #col['width'] / 2
            if i == current_col:
                self.create_rectangle(x1, 0, x2, self.height,
                                      outline='white',
                                      fill='#aaaaaa',
                                      width=1,
                                      tag='header-border')
            else:
                self.create_rectangle(x1, 0, x2, self.height,
                                      outline='white',
                                      width=1,
                                      tag='header-border')

            self.create_text(
                x_center + pad, self.ps['column_header_height']/2,
                text=col['label'],
                anchor='w',
                fill='white',
                #font=self.thefont,
                tag='header-text'
            )

        #x=self.table.col_positions[col+1]
        #self.create_line(x,0, x,h, tag='gridline',
        #                fill='white', width=2)
        return

class RowIndex(tk.Canvas):

    def __init__(self, parent, width=None, bg=''):
        if not width:
            self.width = 60
        else:
            self.width = width
        super().__init__(parent, bg=bg, width=self.width, bd=0, relief='flat')

        self.parent = parent
        self.ps = parent.state

        # saved row index control action (unit: row number start from 0)
        self.selected = {
            'mode': '',
            'row_list': [],
            'row_start': None,
            'row_end': None,
        }
        self.bind('<B1-Motion>', self.handle_mouse_drag)
        self.bind('<Button-1>', self.handle_mouse_button_1)
        self.bind('<Button-3>', self.handle_mouse_button_3)
        self.bind('<Control-Button-1>', self.handle_ctrl_button_1)
        self.bind('<Shift-Button-1>', self.handle_shift_button_1)

    def get_cleaned_row(self, event_y):
        y = int(self.canvasy(event_y))
        row = int(y / self.ps['cell_height'])
        if row >= self.ps['num_rows'] or row < 0:
            return -1
        return row

    def handle_shift_button_1(self, event):
        logging.debug('shift select, {}'.format(self.selected))

        rows = self.parent.get_row_list()
        current_row = self.get_cleaned_row(event.y)
        last_row = rows[0]
        self.selected.update({
            'row_list': list(range(last_row, current_row+1)),
            'mode': 'shift',
        })
        #if self.selected.get('mode', '') in ('click', ''):
        #     if row_start := self.selected['row_start']:
        #         self.selected.update({
        #             'row_list': list(range(row_start, row+1)),
        #             'mode': 'shift',
        #         })
        self.parent.main_table.clear_selected(False)
        self.render_row_highlight()

    def handle_ctrl_button_1(self, event):

        row = self.get_cleaned_row(event.y)
        if row < 0:
            return None

        self.selected.update({
            'mode': 'ctrl-click',
            'x': event.x,
            'y': event.y,
        })

        if row not in self.selected['row_list']:
            self.selected['row_list'].append(row)

        self.render_row_highlight()

        logging.debug('ctrl_button_1 <Control-Button-1>: {}'.format(self.selected))

    def handle_mouse_button_1(self, event):
        self.parent.main_table.clear_selected()

        row = self.get_cleaned_row(event.y)
        if row < 0:
            return None

        self.selected.update({
            'mode': 'click',
            'row_start': row,
            'row_end': row,
            'row_list': [row],
            'x': event.x,
            'y': event.y,
        })
        logging.debug('mouse_button_1 <Button-1>: {}'.format(self.selected))

        self.render_row_highlight()

    def foo(self):
        pass

    def handle_mouse_button_3(self, event):
        self.parent.main_table.render_popup_menu(event)

    def get_selected_rows(self):
        s = self.selected
        rows = []
        mode = s.get('mode', '')
        if mode == 'click':
            rows = s['row_list']
        elif mode == 'drag':
            if s['row_end'] >= 0 or s['row_start'] >= 0:
                diff = s['row_end'] - s['row_start']
                rows = list(range(s['row_start'], s['row_start'] + diff + 1))
        elif mode == 'ctrl-click':
            rows = s['row_list']
        elif mode == 'shift':
            rows = s['row_list']
        return rows

    def handle_mouse_drag(self, event):
        #self.parent.main_table.clear_selected()

        row = self.get_cleaned_row(event.y)
        if row < 0:
            return None

        # prevent drag if in ctrl-click
        if self.selected.get('mode', '') == 'ctrl-click':
            return None

        if abs(self.selected['x']-event.x) <= 3 or abs(self.selected['y']-event.y) <= 3:
           # drag 不夠, 不算 drag (prevent 誤觸)
            return None

        self.selected['mode'] = 'drag'

        self.selected.update({
            'mode': 'drag',
            'row_end': row,
        })
        logging.debug('mouse_drag <B1-Motion>: {}'.format(self.selected))
        self.render_row_highlight()

    def render_row_highlight(self):
        self.delete('row-highlight')

        s = self.selected

        y1 = -1
        y2 = -1
        y_pos_list = [] # for row_index bg color
        rows = [] # for main_table row highlight
        mode = s.get('mode', '')
        if mode == 'click':
            #rows.append(s['row_start'])
            y1 = self.ps['cell_height'] * s['row_start']
            y2 = self.ps['cell_height'] * (s['row_start'] + 1)
            y_pos_list.append((y1, y2))
            rows = s['row_list']
        elif mode == 'drag':
            if s['row_start'] >= 0 and s['row_end'] >= 0:
                diff = s['row_end'] - s['row_start']
                # 不給逆向 (由下往上選)
                if diff > 0:
                    #print (self.ps['cell_height'] * s['row_start'], self.ps['cell_height'] * s['row_end'])
                    y1 = self.ps['cell_height'] * s['row_start']
                    y2 = self.ps['cell_height'] * (s['row_end'] + 1)
                    y_pos_list.append((y1, y2))

                    if diff > 0:
                        rows = list(range(s['row_start'], s['row_end']+1))
                    #elif diff < 0:
                    #    rows = list(range(s['row_end'], s['row_start']+1))

        elif mode == 'ctrl-click':
            for row in s['row_list']:
                y1 = self.ps['cell_height'] * row
                y2 = self.ps['cell_height'] * (row + 1)
                y_pos_list.append((y1, y2))
                rows.append(row)
        elif mode == 'shift':
            for row in s['row_list']:
                y1 = self.ps['cell_height'] * row
                y2 = self.ps['cell_height'] * (row + 1)
                y_pos_list.append((y1, y2))
                rows.append(row)

        for y_pos in y_pos_list:
            self.create_rectangle(
                0, y_pos[0], self.width, y_pos[1],
                fill=self.ps['style']['color']['row-index-highlight'],
                width=2,
                #outline=self.ps['style']['color']['cell-highlight-border'],
                #stipple="gray50",
                tags=('row-highlight'))
        self.lower('row-highlight')

        if func := self.ps.get('after_row_index_selected', ''):
            return func(rows)

    def clear_selected(self):
        self.delete('row-highlight')
        self.selected = {}

    def render(self, current_row=''):
        self.configure(scrollregion=(0,0, self.width, self.ps['height']+30))
        self.delete('header-border')
        self.delete('header-text')

        for i, v in enumerate(self.ps['data'].items()):
            x = self.width - 10
            y1 = i * self.ps['cell_height']
            y2 = (i+1) * self.ps['cell_height']

            if i == current_row:
                self.create_rectangle(0, y1, self.width-1, y2,
                                      outline='white',
                                      fill='#aaaaaa',
                                      width=1,
                                      tag='header-border')
            else:
                self.create_rectangle(0, y1, self.width-1, y2,
                                      outline='white',
                                      width=1,
                                      tag='header-border')


            disp = self.ps.get('row_index_display', '')
            text = ''
            if disp == 'iid':
                text = f'{i+1}({v[0]})', #f'{i+1}'
            else:
                text = v[1][disp]
            self.create_text(x, i*self.ps['cell_height'] + self.ps['cell_height']/2,
                             text=text,
                             fill='white',
                             #font=self.table.thefont,
                             tag='header-text', anchor='e')

class AutoScrollbar(ttk.Scrollbar):
    """a scrollbar that hides itself if it's not needed.  only
       works if you use the grid geometry manager."""

    def set(self, lo, hi):
        #print (lo, hi)
        '''
        if float(lo) <= 0.0 and float(hi) >= 1.0:
            # grid_remove is currently missing from Tkinter!
            self.tk.call("grid", "remove", self)
        else:
            self.grid()
        '''
        ttk.Scrollbar.set(self, lo, hi)
    #def pack(self, **kw):
    #    raise TclError, "cannot use pack with this widget"
    #def place(self, **kw):
    #    raise TclError, "cannot use place with this widget"
