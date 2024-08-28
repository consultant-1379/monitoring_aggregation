package com.ericsson.etl.monitoring.aggregation;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.distocraft.dc5000.repository.cache.AggregationRule;
import com.distocraft.dc5000.repository.cache.AggregationRuleCache;
import com.distocraft.dc5000.repository.cache.AggregationStatus;
import com.distocraft.dc5000.repository.cache.AggregationStatusCache;

public class EventDrivenAggregatorNomination {
	
	private Logger log = null;

	public EventDrivenAggregatorNomination(Logger log) {
		this.log = log;
	}
	
	public void triggerDependantAggregations(AggregationStatus aggSta) throws Exception {
		
		log.info("Executing the aggregator nomination!!");
	    HashMap<String, AggregationRule> aggregationSetsToBeNominated = new HashMap<String, AggregationRule>(); 
	    
	    final List<AggregationRule> rulez = AggregationRuleCache.getCache().getAggregationRules(aggSta.AGGREGATION);
	    
	    log.info("# Vector size is "+rulez.size());

		for(AggregationRule aggregationRule : rulez) {
    	
	    	List<AggregationRule> dependantAggregation = AggregationRuleCache.getCache().getAggregationRules(aggregationRule.getTarget_type(), aggregationRule.getTarget_level());
	    	log.info(" # dependantAggregation size:"+dependantAggregation.size());
	    	for(AggregationRule dependantaggregationRule : dependantAggregation) {
	    		if(!aggregationSetsToBeNominated.containsKey(dependantaggregationRule.getAggregation()) && dependantaggregationRule.getAggregationscope().equalsIgnoreCase(aggregationRule.getAggregationscope()) && !dependantaggregationRule.getTarget_level().equalsIgnoreCase(aggregationRule.getTarget_level())) {
	    			aggregationSetsToBeNominated.put(dependantaggregationRule.getAggregation(), dependantaggregationRule);
	    		}
	    	}
	    }
		try {
	    	
	    	log.info("# aggregationSetsToBeNominated size is "+aggregationSetsToBeNominated.size());
	    	
	    	for(Map.Entry<String, AggregationRule> aggregationSet : aggregationSetsToBeNominated.entrySet()) {
	    		 AggregationRule aggRule = aggregationSet.getValue();
	    		 final AggregationStatus dependantAggStatus = AggregationStatusCache.getStatus(aggRule.getAggregation(), aggSta.DATADATE);
	    		 if(dependantAggStatus != null) {
		    			if(!dependantAggStatus.STATUS.equalsIgnoreCase("BLOCKED") | !dependantAggStatus.STATUS.equalsIgnoreCase("LOADED")) {
		    				dependantAggStatus.STATUS = "LOADED";
		    				dependantAggStatus.THRESHOLD = 0;
		    				dependantAggStatus.LOOPCOUNT = 0;
		    				AggregationStatusCache.setStatus(dependantAggStatus);
		    				log.info("Nominated "+dependantAggStatus.AGGREGATION+" for date"+dependantAggStatus.DATADATE+" by "+aggSta.AGGREGATION);
		    			} else {
		    				log.info("Nomination is not required as "+dependantAggStatus.AGGREGATION+" is already in LOADED/BLOCKED status("+dependantAggStatus.STATUS+")!");
		    			}
		    		} else {
		    			log.info("No depenent aggregation set found for "+aggSta);
		    		}
	    	}
	    		    	
	    } catch(Exception e) {
	    	log.severe("Error while nominating the dependant aggregator set for "+aggSta.AGGREGATION+":"+e);
	    	throw new Exception("Could not able to nominate the dependant aggregation set for "+aggSta.AGGREGATION);
	    } 
		
	}
}
