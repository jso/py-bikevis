class cdf:
    """Implements a simple binning system. Generates cdfs."""
    
    def __init__(self, resolution=1.0, step=True):
        self.step = step
        self.resolution = resolution
        self.binFraction = 1.0 / float(resolution)
        self.addedCount = 0
        self.bins = {}
    
    def insert(self, value, count=1):
        # get the bin
        bin = int(value * self.binFraction)
        remainder = (value * self.binFraction) - bin
        if value < 0 and remainder != 0: bin -= 1
        
        self.bins[bin] = self.bins.get(bin, 0) + count
        
        self.addedCount += count
    
    def pad(self, count):
        """
        Pad the data in the CDF with additional "values" to make the CDF not end at 1.0 
        """
        
        self.addedCount += count

    def getData(self, complement=False):
        xs = []
        ys = []

        binList = self.bins.keys()
        binList.sort()
        
        fractionSoFar = 0.0
        
        for b in binList:
            lowerBound = float(b) / self.binFraction

            if self.step:
                xs.append(lowerBound - ((0.000001 * self.binFraction) / self.binFraction))
                if complement:
                    ys.append(1 - fractionSoFar)
                else:
                    ys.append(fractionSoFar)
           
            fractionSoFar += float(self.bins[b]) / self.addedCount
            
            xs.append(lowerBound)
            if complement:
                ys.append(1 - fractionSoFar)
            else:
                ys.append(fractionSoFar)

        return xs, ys

    def getPdfData(self, complement=False):
        xs = []
        ys = []

        binList = self.bins.keys()
        binList.sort()
        
        for b in binList:
            lowerBound = float(b) / self.binFraction

            f = float(self.bins[b]) / self.addedCount
            
            xs.append(lowerBound)
            if complement:
                ys.append(1 - f)
            else:
                ys.append(f)

        return xs, ys
