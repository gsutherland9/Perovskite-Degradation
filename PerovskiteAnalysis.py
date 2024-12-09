
#####PEROVSKITE DATABASE ANALYSIS TO QUANTIFY RELATIONSHIP BETWEEN EXTRINSIC FACTORS AND PEROVSKITE DEGRADATION

import pandas as pd
import statsmodels.api as sm
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
import numpy as np
from sklearn.metrics import mean_squared_error
from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
from sklearn.preprocessing import OneHotEncoder
import seaborn as sns

###IMPORT THE PEROVSKITE DATABASE CSV 2020 AND ON WITH STABILITY DATA 
df = pd.read_csv('PerovsiteDatabaseQuery2020On.csv')

###CLEAN MISSING DATA
df_cleaned = df.replace(['Unknown', False,'nan; nan', '<NA>', 'NaN'], pd.NA).dropna(axis=1, how='all')
df_cleaned = df_cleaned[df_cleaned['Stability_relative_humidity_average_value'].notna()]
df_cleaned = df_cleaned[df_cleaned['Stability_temperature_range'].notna()]

###SELECT THE NECESSARY COLUMNS
df_analysis = df_cleaned[['Cell_stack_sequence', 'Cell_area_measured', 'Cell_architecture', 'Substrate_stack_sequence', 'ETL_stack_sequence', 'ETL_deposition_procedure', 'ETL_deposition_aggregation_state_of_reactants', 
                         'Perovskite_dimension_3D', 'Perovskite_dimension_list_of_layers', 'Perovskite_composition_perovskite_ABC3_structure', 'Perovskite_composition_a_ions', 'Perovskite_composition_a_ions_coefficients',
                         'Perovskite_composition_b_ions', 'Perovskite_composition_b_ions_coefficients', 'Perovskite_composition_c_ions', 'Perovskite_composition_c_ions_coefficients', 
                         'Perovskite_composition_none_stoichiometry_components_in_excess', 'Perovskite_composition_short_form', 'Perovskite_composition_long_form', 'Perovskite_additives_compounds', 'Perovskite_additives_concentrations',
                         'Perovskite_band_gap', 'Perovskite_band_gap_estimation_basis', 'Perovskite_deposition_procedure', 'Perovskite_deposition_aggregation_state_of_reactants', 'Perovskite_deposition_synthesis_atmosphere',
                         'Perovskite_deposition_solvents', 'Perovskite_deposition_solvents_mixing_ratios', 'Perovskite_deposition_quenching_induced_crystallisation', 'Perovskite_deposition_quenching_media',
                         'Perovskite_deposition_thermal_annealing_temperature', 'Perovskite_deposition_thermal_annealing_time', 'HTL_stack_sequence', 'HTL_thickness_list', 'HTL_additives_compounds', 'HTL_deposition_procedure', 
                         'HTL_deposition_aggregation_state_of_reactants', 'Backcontact_stack_sequence', 'Backcontact_thickness_list', 'JV_measured', 'JV_average_over_n_number_of_cells', 'JV_test_atmosphere',
                         'JV_light_intensity', 'JV_light_spectra', 'JV_reverse_scan_PCE', 'JV_forward_scan_PCE', 'JV_default_PCE', 'JV_default_PCE_scan_direction', 'JV_hysteresis_index', 'Stabilised_performance_measured',
                         'Stabilised_performance_PCE', 'Stability_measured', 'Stability_protocol', 'Stability_light_source_type', 'Stability_light_intensity', 'Stability_light_spectra', 'Stability_potential_bias_load_condition', 
                         'Stability_temperature_range', 'Stability_atmosphere', 'Stability_relative_humidity_average_value', 'Stability_time_total_exposure', 'Stability_PCE_initial_value', 'Stability_PCE_burn_in_observed',
                         'Stability_PCE_end_of_experiment', 'Stability_PCE_T80', 'Stability_PCE_after_1000_h']]

###CALC PCE LOSS/HR METRIC AND SELECT TEMPARATURE FROM 'TEMPERATURE RANGE' (ALL EXPERIMENTS STAYED WITHIN SAME RANGE EX: 25-25)
df_analysis['Stability_temperature'] = df_analysis['Stability_temperature_range'].str[:2]
df_analysis['PCE Loss/Hr'] = (100 - df['Stability_PCE_end_of_experiment']) / df_analysis['Stability_time_total_exposure']

###MAKE NUMERIC
df_analysis[['Cell_area_measured','Stability_temperature', 'Stability_relative_humidity_average_value','Stability_time_total_exposure', 'PCE Loss/Hr']] = \
    df_analysis[['Cell_area_measured','Stability_temperature', 'Stability_relative_humidity_average_value','Stability_time_total_exposure', 'PCE Loss/Hr']].apply(pd.to_numeric)

###FINAL DATAFRAME FOR REGRESSION, ADD/REMOVE GROUPINGS AND AVERAGES AS NEEDED
basedf = (
    df_analysis[['Cell_stack_sequence', 'Perovskite_composition_short_form', 'Cell_architecture', 
                'Stability_protocol', 'Stability_light_source_type', 'Stability_light_intensity', 'Cell_area_measured', 
                'Stability_temperature', 'Stability_relative_humidity_average_value','Stability_time_total_exposure', 'PCE Loss/Hr']] \
)

#KEEP METHYLAMMONIUM LEAD IODIDE FOR MODEL IMPROVEMENT
basedf = basedf[basedf['Perovskite_composition_short_form'] == 'MAPbI']
basedf = basedf.reset_index(drop=True)
basedf.loc[basedf['Stability_light_source_type'] == 'UV lamp', 'Stability_light_intensity'] = 100


##REGRESSION/TRAIN TEST SPLIT
##VARIABLES
y = basedf['PCE Loss/Hr']
X = basedf.drop(columns=['Cell_stack_sequence', 'Perovskite_composition_short_form', 'Cell_architecture', 'Stability_protocol', 'Stability_light_source_type', 'Cell_area_measured', 'PCE Loss/Hr'])

X = sm.add_constant(X)

#SPLIT
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

#OLS REGRESSION
model = sm.OLS(y_train, X_train)
results = model.fit()

print(results.summary())

#PREDICT TEST SET
y_pred = results.predict(X_test)

#COMPUTE VIF
vif_data = pd.DataFrame()
vif_data["Variable"] = X.columns
vif_data["VIF"] = [variance_inflation_factor(X.values, i) for i in range(X.shape[1])]
print(vif_data[['Variable', 'VIF']])

#R SQUARED
r2 = r2_score(y_test, y_pred)
print(f"R-squared: {r2:.4f}")

#MEAN SQUARED ERROR
mse = mean_squared_error(y_test, y_pred)
print(f"Mean Squared Error: {mse:.4f}")

#RMSE
rmse = np.sqrt(mse)
print(f"Root Mean Squared Error: {rmse:.4f}")
 
#RESIDUALS ANALYSIS
residuals = y_test - y_pred
print(f"Residuals:\n{residuals[:10]}")  # Display first 10 residuals
print(f"%Off:\n{(residuals/y_test)*100}")
print(f"%y_test:\n{(y_test)}")
print(f"%y_pred:\n{(y_pred)}")

#RESIDUALS SCATTERPLOT
df_resid = pd.DataFrame({'y_test': y_test, 'y_pred': y_pred})

plt.figure(figsize=(8, 6))

sns.scatterplot(x=np.arange(len(y_test)), y=y_test, color='blue', label='y_test')
sns.scatterplot(x=np.arange(len(y_pred)), y=y_pred, color='red', label='y_pred')

plt.xlabel('Index')
plt.ylabel('Value')
plt.title('Scatterplot of y_test and y_pred')
plt.legend()
plt.show()















