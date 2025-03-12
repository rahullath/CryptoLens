"""
Visualizer Module for Crypto Revenue Analyzer

This module creates visualizations for the collected data:
1. Protocol comparison table
2. Bubble map of revenue contributions by blockchain
"""

import os
import json
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from config import PROTOCOLS, NETWORKS

class Visualizer:
    def __init__(self, data_dir="../data", output_dir="../visualizations"):
        """Initialize the Visualizer."""
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.ensure_dirs()
        
    def ensure_dirs(self):
        """Ensure the data and output directories exist."""
        for directory in [self.data_dir, self.output_dir]:
            if not os.path.exists(directory):
                os.makedirs(directory)
    
    def load_data(self, filename):
        """Load data from JSON file."""
        filepath = os.path.join(self.data_dir, filename)
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                return json.load(f)
        return None
    
    def create_protocol_comparison_table(self):
        """
        Create a comparison table for protocols.
        
        Metrics:
        - Market Cap
        - Annual Revenue
        - QoQ Revenue Growth
        - Sustainability
        - Token Type
        - Overall Rating
        """
        print("Creating protocol comparison table...")
        
        # Load data
        protocol_data = self.load_data("protocol_analysis_data.json")
        revenue_data = self.load_data("all_protocols_revenue_data.json")
        
        if not protocol_data or not revenue_data:
            print("Required data files not found.")
            return None
        
        # Prepare data for the table
        table_data = []
        
        for protocol in PROTOCOLS:
            protocol_name = protocol['name']
            
            # Get protocol info
            protocol_info = protocol_data.get(protocol_name, {})
            
            # Get revenue info
            protocol_revenue = revenue_data.get(protocol_name, {})
            
            # Calculate total annual revenue across all chains
            annual_revenue = 0
            for chain, chain_data in protocol_revenue.items():
                annual_revenue += chain_data.get('yearly_projection', 0)
            
            # Get other metrics
            market_cap = protocol_info.get('market_cap', 'N/A')
            qoq_growth = protocol_info.get('qoq_growth', {}).get('qoq_growth_pct', 'N/A')
            sustainability = protocol_info.get('sustainability', {}).get('sustainability_score', 'N/A')
            token_type = protocol_info.get('token_type', 'N/A')
            
            # Calculate overall rating (simplified example)
            # In a real implementation, this would be more sophisticated
            if isinstance(sustainability, (int, float)) and isinstance(qoq_growth, (int, float)):
                # Simple rating based on sustainability and growth
                if sustainability >= 75 and qoq_growth > 10:
                    overall_rating = "Excellent"
                elif sustainability >= 50 and qoq_growth > 0:
                    overall_rating = "Good"
                elif sustainability >= 25:
                    overall_rating = "Average"
                else:
                    overall_rating = "Below Average"
            else:
                overall_rating = "N/A"
            
            # Add to table data
            table_data.append({
                "Protocol": protocol_name,
                "Market Cap ($)": market_cap if market_cap != 'N/A' else 0,
                "Annual Revenue ($)": annual_revenue,
                "QoQ Growth (%)": qoq_growth if qoq_growth != 'N/A' else 0,
                "Sustainability Score": sustainability if sustainability != 'N/A' else 0,
                "Token Type": token_type,
                "Overall Rating": overall_rating
            })
        
        # Create DataFrame
        df = pd.DataFrame(table_data)
        
        # Save to CSV
        csv_path = os.path.join(self.output_dir, "protocol_comparison.csv")
        df.to_csv(csv_path, index=False)
        print(f"Protocol comparison table saved to {csv_path}")
        
        # Create a styled HTML table
        html_path = os.path.join(self.output_dir, "protocol_comparison.html")
        
        # Apply styling
        styled_df = df.style.format({
            "Market Cap ($)": "${:,.0f}",
            "Annual Revenue ($)": "${:,.0f}",
            "QoQ Growth (%)": "{:.1f}%",
            "Sustainability Score": "{:.0f}/100"
        })
        
        # Apply conditional formatting
        styled_df = styled_df.background_gradient(cmap='RdYlGn', subset=["Annual Revenue ($)", "QoQ Growth (%)", "Sustainability Score"])
        
        # Save to HTML
        styled_df.to_html(html_path)
        print(f"Styled protocol comparison table saved to {html_path}")
        
        return df
    
    def create_revenue_bubble_map(self):
        """
        Create a bubble map of revenue contributions by blockchain.
        
        Shows which protocols contribute how much revenue on each blockchain.
        """
        print("Creating revenue bubble map...")
        
        # Load data
        revenue_data = self.load_data("all_protocols_revenue_data.json")
        
        if not revenue_data:
            print("Revenue data file not found.")
            return None
        
        # Prepare data for the bubble map
        bubble_data = []
        
        for protocol_name, chains in revenue_data.items():
            for chain, chain_data in chains.items():
                # Only include main networks we're interested in
                if chain.lower() in [network.lower() for network in NETWORKS]:
                    annual_revenue = chain_data.get('yearly_projection', 0)
                    
                    # Skip if revenue is zero
                    if annual_revenue <= 0:
                        continue
                    
                    bubble_data.append({
                        "Protocol": protocol_name,
                        "Blockchain": chain.upper(),
                        "Annual Revenue ($)": annual_revenue
                    })
        
        # Create DataFrame
        df = pd.DataFrame(bubble_data)
        
        # Calculate total revenue per blockchain
        blockchain_totals = df.groupby('Blockchain')['Annual Revenue ($)'].sum().reset_index()
        blockchain_totals.columns = ['Blockchain', 'Total Revenue ($)']
        
        # Calculate percentage contribution
        df = df.merge(blockchain_totals, on='Blockchain')
        df['Contribution (%)'] = (df['Annual Revenue ($)'] / df['Total Revenue ($)']) * 100
        
        # Save to CSV
        csv_path = os.path.join(self.output_dir, "revenue_contributions.csv")
        df.to_csv(csv_path, index=False)
        print(f"Revenue contributions data saved to {csv_path}")
        
        # Create bubble map using Plotly
        fig = px.scatter(
            df,
            x="Blockchain",
            y="Protocol",
            size="Annual Revenue ($)",
            color="Blockchain",
            hover_name="Protocol",
            hover_data={
                "Annual Revenue ($)": ":,.0f",
                "Contribution (%)": ":.1f",
                "Blockchain": False
            },
            title="Revenue Contributions by Blockchain",
            size_max=60
        )
        
        # Update layout
        fig.update_layout(
            xaxis_title="Blockchain",
            yaxis_title="Protocol",
            legend_title="Blockchain",
            font=dict(size=12),
            height=600,
            width=800
        )
        
        # Save as HTML
        html_path = os.path.join(self.output_dir, "revenue_bubble_map.html")
        fig.write_html(html_path)
        print(f"Revenue bubble map saved to {html_path}")
        
        # Save as PNG
        png_path = os.path.join(self.output_dir, "revenue_bubble_map.png")
        fig.write_image(png_path)
        print(f"Revenue bubble map saved to {png_path}")
        
        return df
    
    def create_all_visualizations(self):
        """Create all visualizations."""
        self.create_protocol_comparison_table()
        self.create_revenue_bubble_map()
        print("All visualizations created!")


if __name__ == "__main__":
    visualizer = Visualizer()
    visualizer.create_all_visualizations()
