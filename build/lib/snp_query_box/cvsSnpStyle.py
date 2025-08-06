import seaborn as sns
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap

cvs_color_palette_nested = {
    "Red": {
        "10": "#FF9D99", "20": "#FF9383", "30": "#FF8C8C", "40": "#FA5757",
        "50": "#EB0000", "60": "#CC0000", "70": "#A50000", "80": "#730B0B", "90": "#4D0F0F"
    },
    "Violet": {
        "10": "#DBC3E5", "20": "#CCA7DB", "30": "#B587C9", "40": "#A59DBD",
        "50": "#9556B0", "60": "#7D3F98", "70": "#6E3796", "80": "#5A2D6D", "90": "#462254"
    },
    "Teal": {
        "10": "#A7EBE6", "20": "#6FDEDE", "30": "#33CCCC", "40": "#03BFB2",
        "50": "#0AA69C", "60": "#0BB57C", "70": "#0A6B64", "80": "#0F4F4A", "90": "#073330"
    },
    "Indigo": {
        "10": "#C7E3FF", "20": "#91C8FF", "30": "#5CADFF", "40": "#1C8EFF",
        "50": "#0073E3", "60": "#0060BF", "70": "#004F9E", "80": "#00337B", "90": "#0B31E5"
    },
    "Sky": {
        "10": "#AEEAF5", "20": "#73DEF0", "30": "#3BD1EB", "40": "#00C7EB",
        "50": "#00A8C7", "60": "#0082B9", "70": "#02697A", "80": "#0B5A6B", "90": "#0A3840"
    },
    "Forest": {
        "10": "#BAF29D", "20": "#89E55C", "30": "#5FCC29", "40": "#4DB21B",
        "50": "#3D890F", "60": "#36870D", "70": "#2A7306", "80": "#1E5900", "90": "#154000"
    },
    "Tangerine": {
        "10": "#FFDB82", "20": "#FFB273", "30": "#FF8F33", "40": "#FF7300",
        "50": "#E58346", "60": "#BF5600", "70": "#A64B00", "80": "#8C3F00", "90": "#662E00"
    },
    "Canary": {
        "10": "#FCEBA7", "20": "#F9DD6E", "30": "#F3D027", "40": "#F9C016",
        "50": "#D9A004", "60": "#BB8907", "70": "#977709", "80": "#755C09", "90": "#54420B"
    },
    "Rose": {
        "10": "#FFCC00", "20": "#FFA0C4", "30": "#FF80AA", "40": "#FF6C93",
        "50": "#FF3B73", "60": "#CF3F6F", "70": "#A62953", "80": "#800040", "90": "#59162C"
    },
    "Dune": {
        "10": "#FAE7CB", "20": "#F5D6A4", "30": "#EDC687", "40": "#EDB269",
        "50": "#BDB848", "60": "#91713D", "70": "#708A36", "80": "#54463D", "90": "#2E2B1E"
    }
}

def get_cvs_color_palette(cvs_color_palette_nested=cvs_color_palette_nested):
    from DsnpHelperFunction import flatten_dict
    return flatten_dict(cvs_color_palette_nested)

gray_levels = {
    "10": "#E7E7E7", "20": "#DEDEDE", "30": "#CCCCCC", "40": "#B8B8B8", 
    "50": "#A3A3A3", "60": "#8F8F8F", "70": "#767676", "80": "#575757", 
    "90": "#474747"
}

def maximally_differentiated_palette(color_family="Red", n_colors=4):
    """
    Creates a palette with maximally differentiated colors from a single color family.
    Caps intensity at 70 for 5 or fewer colors for better aesthetics.
    
    Args:
        color_family (str): The color family to use (Red, Violet, Teal, etc.)
        n_colors (int): Number of colors to include (1-9)
    
    Returns:
        list: A list of hex color codes that are maximally spaced
    """
    color_dict = cvs_color_palette_nested.get(color_family)
    if not color_dict:
        if color_family == "Gray":
            color_dict = gray_levels
        else:
            raise ValueError(f"Unknown color family: {color_family}")
    
    # If 5 or fewer colors, use intensities 10 through 70
    if n_colors <= 5:
        available_intensities = ["10", "20", "30", "40", "50", "60", "70"]
    else:
        # For more than 5 colors, include intensities up to 90
        available_intensities = ["10", "20", "30", "40", "50", "60", "70", "80", "90"]
    
    # Ensure we don't request more colors than available
    n_colors = min(n_colors, len(available_intensities))
    
    if n_colors == 1:
        # Just return the middle intensity
        return [color_dict["50"]]
    
    if n_colors == 2:
        # For two colors, return lightest and darkest from our available range
        return [
            color_dict[available_intensities[0]],
            color_dict[available_intensities[-1]]
        ]
    
    # For more colors, select evenly spaced intensities
    indices = np.linspace(0, len(available_intensities)-1, n_colors).astype(int)
    selected_intensities = [available_intensities[i] for i in indices]
    
    return [color_dict[intensity] for intensity in selected_intensities]

def enterprise_default_palette(n_colors=4):
    """
    Creates a default palette using one shade from each color family 
    in the Enterprise Color System.
    
    Args:
        n_colors (int): Number of colors to include
        
    Returns:
        list: A list of hex color codes from different color families
    """
    # Use a distinct shade from each color family for maximum differentiation
    palette = [
        cvs_color_palette_nested["Red"]["60"],
        cvs_color_palette_nested["Teal"]["50"],
        cvs_color_palette_nested["Violet"]["60"],
        cvs_color_palette_nested["Indigo"]["50"],
        cvs_color_palette_nested["Tangerine"]["60"],
        cvs_color_palette_nested["Forest"]["50"],
        cvs_color_palette_nested["Sky"]["50"],
        cvs_color_palette_nested["Rose"]["60"],
        cvs_color_palette_nested["Canary"]["50"],
        cvs_color_palette_nested["Dune"]["60"]
    ]
    
    return palette[:min(n_colors, len(palette))]

def enterprise_plot_style(color_mode="default", primary_color="Red", n_colors=4, 
                          contrasting_pairs=None):
    """
    Sets up the Enterprise Color System styling for data visualization with flexible color options.
    
    Args:
        color_mode (str): Color strategy to use:
                         - "default": Uses colors from different families for max contrast
                         - "single_hue": Uses maximally differentiated shades of one color
                         - "contrasting": Uses contrasting color pairs
        primary_color (str): Primary color family to use (Red, Violet, Teal, etc.)
                           Only used for "single_hue" or as first color in "contrasting"
        n_colors (int): Number of colors needed for the visualization
        contrasting_pairs (list): List of tuples specifying color pairs for "contrasting" mode
                                Default is [(primary_color, "Gray")]
    
    Returns:
        list: The palette used
    """
    custom_params = {
        "axes.spines.right": False,
        "axes.spines.top": False
    }
    
    # Set basic seaborn theme
    sns.set_theme(
        context="notebook",
        style="whitegrid", 
        font_scale=1.05,
        font="CVS Health Sans", 
        rc=custom_params
    )
    
    # Default Enterprise palette - one shade from each color family
    if color_mode == "default":
        palette = enterprise_default_palette(n_colors)
    
    # Maximally differentiated shades of a single color
    elif color_mode == "single_hue":
        palette = maximally_differentiated_palette(primary_color, n_colors)
    
    # Contrasting color pairs
    elif color_mode == "contrasting":
        # If we need more than 9 colors, fall back to default enterprise palette
        if n_colors > 9:
            return enterprise_default_palette(n_colors)
            
        # Default contrasting colors if none provided - ensure variety in defaults
        if contrasting_pairs is None:
            # Use different color families that provide good contrast
            default_pairs = [
                (primary_color, "Gray"),       # User's primary choice with gray
                ("Teal", "Rose"),              # Cool/warm contrast
                ("Indigo", "Tangerine"),       # Blue/orange contrast
                ("Violet", "Forest"),          # Purple/green contrast
                ("Sky", "Canary")              # Light blue/yellow contrast
            ]
            
            # Only use as many pairs as needed (but at least 1)
            pairs_needed = max(1, (n_colors + 1) // 2)  # Ceiling division
            contrasting_pairs = default_pairs[:pairs_needed]
        
        # For contrasting pairs, we'll use a completely different approach
        # that ensures no duplicates and maximizes differentiation
        palette = []
        
        # First, determine how many colors we need from each pair
        colors_per_pair = {}
        
        # Initial allocation - try to distribute evenly
        base_count = n_colors // len(contrasting_pairs)
        remainder = n_colors % len(contrasting_pairs)
        
        for i, pair in enumerate(contrasting_pairs):
            # Distribute remainder across pairs
            count = base_count + (1 if i < remainder else 0)
            colors_per_pair[pair] = count
        
        # Now, build the palette by selecting appropriate intensities from each pair
        for pair, count in colors_per_pair.items():
            color1, color2 = pair
            
            if count == 1:
                # Just use the primary color at 60% intensity
                if color1 in cvs_color_palette_nested:
                    palette.append(cvs_color_palette_nested[color1]["60"])
                elif color1 == "Gray":
                    palette.append(gray_levels["60"])
                else:
                    palette.append(color1)
                    
            elif count == 2:
                # Use both colors at their middle intensities
                if color1 in cvs_color_palette_nested:
                    palette.append(cvs_color_palette_nested[color1]["60"])
                elif color1 == "Gray":
                    palette.append(gray_levels["60"])
                else:
                    palette.append(color1)
                    
                if color2 in cvs_color_palette_nested:
                    palette.append(cvs_color_palette_nested[color2]["40"])
                elif color2 == "Gray":
                    palette.append(gray_levels["40"])
                else:
                    palette.append(color2)
                    
            else:
                # More complex distribution - alternate between the two colors
                # Select different intensities to maximize differentiation
                
                # For the first color, use these intensities
                intensities1 = ["60", "40", "70"]
                # For the second color, use these intensities
                intensities2 = ["50", "30", "60"]
                
                # Interleave colors with different intensities
                for j in range(count):
                    if j % 2 == 0:
                        # Use color1 with varying intensity
                        intensity_idx = (j // 2) % len(intensities1)
                        if color1 in cvs_color_palette_nested:
                            palette.append(cvs_color_palette_nested[color1][intensities1[intensity_idx]])
                        elif color1 == "Gray":
                            palette.append(gray_levels[intensities1[intensity_idx]])
                        else:
                            palette.append(color1)
                    else:
                        # Use color2 with varying intensity
                        intensity_idx = (j // 2) % len(intensities2)
                        if color2 in cvs_color_palette_nested:
                            palette.append(cvs_color_palette_nested[color2][intensities2[intensity_idx]])
                        elif color2 == "Gray":
                            palette.append(gray_levels[intensities2[intensity_idx]])
                        else:
                            palette.append(color2)
    
    else:
        raise ValueError(f"Unknown color_mode: {color_mode}. Use 'default', 'single_hue', or 'contrasting'.")
    
    # Set the palette
    sns.set_palette(palette)
    
    return palette

# Example usage
if __name__ == "__main__":
    # Create sample data for demonstration
    categories = ['Q1', 'Q2', 'Q3', 'Q4']
    regions = ['Northeast', 'Southeast', 'Midwest', 'West']
    
    data = {
        'Quarter': np.repeat(categories, len(regions)),
        'Region': regions * len(categories),
        'Sales': [
            4.2, 5.1, 6.8, 7.2,  # Northeast
            3.8, 4.5, 5.2, 4.9,  # Southeast
            5.3, 5.0, 5.9, 6.5,  # Midwest
            6.1, 7.3, 7.8, 8.2   # West
        ]
    }
    df = pd.DataFrame(data)
    
    # Example 1: Default Enterprise palette - one shade from each color family
    plt.figure(figsize=(12, 6))
    palette = enterprise_plot_style(color_mode="default", n_colors=4)
    sns.barplot(x='Quarter', y='Sales', hue='Region', data=df)
    plt.title('Default Enterprise Palette', fontsize=16)
    plt.tight_layout()
    plt.show()
    
    # Example 2: Single hue (Red) with maximally differentiated intensities
    plt.figure(figsize=(12, 6))
    palette = enterprise_plot_style(color_mode="single_hue", primary_color="Red", n_colors=4)
    sns.barplot(x='Quarter', y='Sales', hue='Region', data=df)
    plt.title('Single Hue (Red) - Maximally Differentiated', fontsize=16)
    plt.tight_layout()
    plt.show()
    
    # Example 3: Contrasting color pairs (Red vs. Gray by default)
    plt.figure(figsize=(12, 6))
    palette = enterprise_plot_style(color_mode="contrasting", primary_color="Red", n_colors=4)
    sns.barplot(x='Quarter', y='Sales', hue='Region', data=df)
    plt.title('Contrasting Colors (Red vs. Gray)', fontsize=16)
    plt.tight_layout()
    plt.show()
    
    # Example 4: Multiple contrasting pairs for 7 regions
    seven_regions = ['Northeast', 'Southeast', 'Midwest', 'West', 'Southwest', 'Northwest', 'Central']
    data_7 = {
        'Quarter': np.repeat(['Q1', 'Q2'], len(seven_regions)),
        'Region': seven_regions * 2,
        'Sales': [4.2, 3.8, 5.3, 6.1, 5.8, 4.5, 5.0, 5.1, 4.5, 5.0, 7.3, 6.0, 4.9, 5.2]
    }
    df_7 = pd.DataFrame(data_7)
    
    plt.figure(figsize=(12, 6))
    palette = enterprise_plot_style(
        color_mode="contrasting", 
        n_colors=7,  # Let it automatically handle 7 groups
        primary_color="Teal"
    )
    sns.barplot(x='Quarter', y='Sales', hue='Region', data=df_7)
    plt.title('Multiple Contrasting Pairs for 7 Regions', fontsize=16)
    plt.tight_layout()
    plt.show()
