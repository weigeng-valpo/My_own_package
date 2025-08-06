import seaborn as sns

aetna_palette = {
    "dark_purple": "#563D82",
    "dark_grey": "#646464",
    "dark_teal": "#00787E",
    "dark_red": "#9E0000",
    "dark_green": "#487A10",
    "dark_navy": "#0B315E",
    "dark_orange": "#CE430C",
    "dark_warm_grey": "#646464",
    "mid_purple": "#7D3F98",
    "mid_grey": "#C0C0C0",
    "med_teal": "#00A78E",
    "light_red": "#CC0000",
    "light_green": "#61A515",
    "royal_blue": "#267AC0",
    "med_navy": "#0A48BC",
    "light_grey": "#E9E9E9",
    "light_navy": "#0A4B8C",
    "light_warm_grey": "#868686",
    "light_orange": "#F4642A"
}

aetna_palette_list = list(aetna_palette.values())

def aetna_dsnp_plot_style():
    """
    This function loads the Aetna color palette and sets default theming for DSNP plots used in reports and slide decks.
    To use, simply run from snp_query_box import dsnp_sns; dsnp_sns.aetna_dsnp_plot_style.

    The order listed in the color palette will be the order used in plot generation. 
    We recommend setting an appropriate aspect ratio for each individual plot based on what is being visualized.

    Ex:

    >>>import seaborn as sns
    >>>import numpy as np
    >>>import matplotlib.pyplot as plt
    >>>from snp_query_box import dsnpStyle
    >>>dsnpStyle.aetna_dsnp_plot_style()
    >>>x=np.random.randn(100)
    >>>g = sns.displot(x)
    >>>plt.show()
    
    """
    sns.set_theme(style="whitegrid", palette="pastel")

    custom_params = {"axes.spines.right": False, 
                     "axes.spines.top": False}
    sns.set_theme(context='notebook',style='whitegrid', font_scale=1.05,font='CVS Health Sans', rc=custom_params)
    sns.set_palette(palette=aetna_palette_list)


def cvs_dsnp_plot_style():
    sns.set_theme(style="whitegrid", palette="pastel")

    custom_params = {"axes.spines.right": False, 
                     "axes.spines.top": False}
    sns.set_theme(context='notebook',style='whitegrid', font_scale=1.05,font='CVS Health Sans', rc=custom_params)
    sns.set_palette(palette=aetna_palette_list)