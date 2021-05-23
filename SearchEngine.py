'''
This creates the GUI and plugs it into the backend
'''
import tkinter as tk

import scripts


window = tk.Tk()
Textbox = tk.Text(window, bd=5)
indexbox = tk.Entry(window)
keywordbox = tk.Entry(window)
def search(args):
    '''
    Uses some globals to get whatever is in the text boxes when you press enter, and then fires off a query for them
    '''
    global Textbox
    Textbox.delete('1.0','end')
    global indexbox
    global keywordbox
    t1 = indexbox.get()
    t2 = keywordbox.get()
    Textbox.insert(tk.END, scripts.query(t1, t2))


def gui():
    '''
    This is a pretty standard tkinter GUI
    '''
    window.configure(background='gray')
    window.title("Search Engine")
    window.geometry("500x500")
    Butt = tk.Button(window,text="Search")
    global indexbox
    indexbox.insert(0, 'Column')

    indexbox.place(
        x = 10,
        y = 2,
        width = 200,
        height = 30
    )
    global keywordbox
    keywordbox.insert(0,"Keyword")
    keywordbox.place(
        x = 220,
        y = 2,
        width = 200,
        height = 30
    )
    Butt.place(
        x = 430,
        y = 2,
        width = 60,
        height = 31

    )
    Textbox.place(
        x = 25,
        y = 35,
        width = 425,
        height = 425

    )
    Butt.bind("<Button-1>", search)
    window.mainloop()



